use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use agent_client_protocol::Error;
use tokio::sync::Mutex;

use crate::{
    config::Config,
    model::{Role, SessionRecord, SessionRuntime},
};

#[derive(Clone, Debug)]
pub struct SessionStore {
    directory: PathBuf,
    sessions: Arc<Mutex<HashMap<String, SessionRuntime>>>,
    deleted: Arc<Mutex<std::collections::HashSet<String>>>,
    write_lock: Arc<Mutex<()>>,
    system_prompt: Arc<str>,
    default_model: Arc<str>,
    default_permission: crate::config::PermissionMode,
    default_reasoning_effort: crate::config::ReasoningEffort,
}

impl SessionStore {
    pub async fn open(config: &Config) -> Result<Self, Error> {
        let directory = config.sessions_dir();
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(Error::into_internal_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(Error::into_internal_error)?;
        }
        Ok(Self {
            directory,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            deleted: Arc::new(Mutex::new(std::collections::HashSet::new())),
            write_lock: Arc::new(Mutex::new(())),
            system_prompt: Arc::from(config.built_in_system_prompt()),
            default_model: Arc::from(config.default_model.as_str()),
            default_permission: config.permission_mode,
            default_reasoning_effort: config.reasoning_effort,
        })
    }

    pub async fn create(
        &self,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        system_prompt: Option<String>,
        parent_session_id: Option<String>,
    ) -> Result<SessionRuntime, Error> {
        validate_roots(&cwd, &additional_directories)?;
        let mut record = SessionRecord::new(
            cwd,
            additional_directories,
            self.default_model.to_string(),
            self.default_permission,
            self.default_reasoning_effort,
            system_prompt,
            parent_session_id,
        );
        record.install_system_prompt(self.effective_system_prompt(&record));
        let runtime = SessionRuntime::new(record.clone());
        self.save_record(&record).await?;
        self.sessions
            .lock()
            .await
            .insert(record.id.clone(), runtime.clone());
        Ok(runtime)
    }

    pub async fn get(&self, id: &str) -> Result<SessionRuntime, Error> {
        if let Some(runtime) = self.sessions.lock().await.get(id).cloned() {
            return Ok(runtime);
        }
        let record = self.read_record(id).await?;
        let mut record = record;
        // Before pending steering is appended after the interrupted calls.
        record.close_interrupted_tool_calls();
        record.flush_pending_steering();
        record.install_system_prompt(self.effective_system_prompt(&record));
        let runtime = SessionRuntime::new(record);
        let mut sessions = self.sessions.lock().await;
        Ok(sessions.entry(id.to_string()).or_insert(runtime).clone())
    }

    pub async fn save(&self, runtime: &SessionRuntime) -> Result<(), Error> {
        let _write_guard = self.write_lock.lock().await;
        let snapshot = runtime.snapshot().await;
        self.write_record(&snapshot).await
    }

    pub async fn save_record(&self, record: &SessionRecord) -> Result<(), Error> {
        let _write_guard = self.write_lock.lock().await;
        self.write_record(record).await
    }

    async fn write_record(&self, record: &SessionRecord) -> Result<(), Error> {
        validate_session_id(&record.id)?;
        if self.deleted.lock().await.contains(&record.id) {
            return Ok(());
        }
        let path = self.record_path(&record.id);
        let temporary = self.directory.join(format!(
            ".{}.{}.tmp",
            record.id,
            uuid::Uuid::new_v4().simple()
        ));
        let bytes = serde_json::to_vec_pretty(record).map_err(Error::into_internal_error)?;
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(Error::into_internal_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
                .await
                .map_err(Error::into_internal_error)?;
        }
        tokio::fs::rename(&temporary, path)
            .await
            .map_err(Error::into_internal_error)?;
        Ok(())
    }

    pub async fn list(&self, cwd: Option<&Path>) -> Result<Vec<SessionRecord>, Error> {
        let mut records = Vec::new();
        let mut directory = tokio::fs::read_dir(&self.directory)
            .await
            .map_err(Error::into_internal_error)?;
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(Error::into_internal_error)?
        {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let bytes = match tokio::fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let Ok(record) = serde_json::from_slice::<SessionRecord>(&bytes) else {
                continue;
            };
            if cwd.is_none_or(|cwd| record.cwd == cwd) {
                records.push(record);
            }
        }
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        Ok(records)
    }

    pub async fn delete(&self, id: &str) -> Result<(), Error> {
        validate_session_id(id)?;
        self.deleted.lock().await.insert(id.to_string());
        let runtime = self.sessions.lock().await.remove(id);
        if let Some(runtime) = runtime {
            runtime
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            runtime.state.lock().await.cancellation.cancel();
        }
        let _write_guard = self.write_lock.lock().await;
        match tokio::fs::remove_file(self.record_path(id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::into_internal_error(error)),
        }
    }

    pub async fn close(&self, id: &str) -> Result<(), Error> {
        validate_session_id(id)?;
        let runtime = self.sessions.lock().await.remove(id);
        if let Some(runtime) = runtime {
            runtime
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            runtime.state.lock().await.cancellation.cancel();
        }
        Ok(())
    }

    pub fn effective_system_prompt(&self, record: &SessionRecord) -> String {
        let mut prompt = self.system_prompt.to_string();
        if let Some(system_prompt) = &record.system_prompt {
            prompt.push_str("\nClient-provided system instructions:\n");
            prompt.push_str(system_prompt);
            prompt.push('\n');
        }
        prompt
    }

    fn record_path(&self, id: &str) -> PathBuf {
        self.directory.join(format!("{id}.json"))
    }

    async fn read_record(&self, id: &str) -> Result<SessionRecord, Error> {
        validate_session_id(id)?;
        let bytes = tokio::fs::read(self.record_path(id))
            .await
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Error::resource_not_found(Some(format!("session://{id}")))
                } else {
                    Error::into_internal_error(error)
                }
            })?;
        serde_json::from_slice(&bytes).map_err(Error::into_internal_error)
    }
}

fn validate_session_id(id: &str) -> Result<(), Error> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Error::resource_not_found(Some(format!("session://{id}"))));
    }
    Ok(())
}

pub fn session_title(record: &SessionRecord) -> Option<String> {
    if let Some(title) = &record.title {
        return Some(title.clone());
    }
    record
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .and_then(|message| message.text_content())
        .map(|content| {
            let single_line = content.split_whitespace().collect::<Vec<_>>().join(" ");
            if single_line.chars().count() > 80 {
                format!("{}...", single_line.chars().take(77).collect::<String>())
            } else {
                single_line
            }
        })
}

pub(crate) fn validate_roots(cwd: &Path, additional: &[PathBuf]) -> Result<(), Error> {
    if !cwd.is_absolute() {
        return Err(Error::invalid_params().data("session cwd must be absolute"));
    }
    if !cwd.is_dir() {
        return Err(Error::resource_not_found(Some(cwd.display().to_string())));
    }
    for path in additional {
        if !path.is_absolute() {
            return Err(Error::invalid_params().data(format!(
                "additional directory must be absolute: {}",
                path.display()
            )));
        }
        if !path.is_dir() {
            return Err(Error::resource_not_found(Some(path.display().to_string())));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PermissionMode, ReasoningEffort};

    #[tokio::test]
    async fn records_round_trip_and_delete() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir: temp.path().join("data"),
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: Some(100),
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(temp.path().to_path_buf(), Vec::new(), None, None)
            .await
            .unwrap();
        let id = runtime.snapshot().await.id;
        assert!(store.get(&id).await.is_ok());
        assert_eq!(store.list(None).await.unwrap().len(), 1);
        store.delete(&id).await.unwrap();
        store.save(&runtime).await.unwrap();
        assert!(store.list(None).await.unwrap().is_empty());
        assert!(store.get("../../outside").await.is_err());
    }
}
