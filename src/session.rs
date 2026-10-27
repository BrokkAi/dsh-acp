use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use agent_client_protocol::Error;
use tokio::sync::Mutex;

use crate::{
    config::{BUILT_IN_SYSTEM_PROMPT, Config},
    model::{ClientPromptMode, ClientSystemPrompt, Role, SessionRecord, SessionRuntime},
};

const MAX_INSTRUCTION_BYTES: u64 = 1024 * 1024;

/// Whether the store knows a session, as `session/status` reports it.
pub enum SessionPresence {
    /// In memory.
    Active(SessionRuntime),
    /// Stored but not in memory.
    Inactive,
    /// Neither in memory nor stored.
    Unknown,
}

#[derive(Clone, Debug)]
pub struct SessionStore {
    directory: PathBuf,
    data_dir: PathBuf,
    sessions: Arc<Mutex<HashMap<String, SessionRuntime>>>,
    deleted: Arc<Mutex<std::collections::HashSet<String>>>,
    /// URL elicitations outstanding on this connection, by `elicitationId`.
    /// The process serves one stdio connection, so this is that connection's
    /// state. It is never persisted: a completion may only be reported to the
    /// Client that received the original request.
    url_elicitations: Arc<Mutex<HashMap<String, UrlElicitation>>>,
    write_lock: Arc<Mutex<()>>,
    configured_prompt: Option<Arc<str>>,
    default_model: Arc<str>,
    default_permission: crate::config::PermissionMode,
    default_reasoning_effort: crate::config::ReasoningEffort,
}

#[derive(Clone, Debug)]
struct UrlElicitation {
    session_id: String,
    /// The user consented to open the URL. The interaction then runs out of
    /// band until the model reports that it finished.
    accepted: bool,
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
            data_dir: config.data_dir.clone(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            deleted: Arc::new(Mutex::new(std::collections::HashSet::new())),
            url_elicitations: Arc::new(Mutex::new(HashMap::new())),
            write_lock: Arc::new(Mutex::new(())),
            configured_prompt: config.configured_system_prompt().map(Arc::from),
            default_model: Arc::from(config.default_model.as_str()),
            default_permission: config.permission_mode,
            default_reasoning_effort: config.reasoning_effort,
        })
    }

    pub async fn create(
        &self,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        system_prompt: Option<ClientSystemPrompt>,
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
        record.install_system_prompt(self.effective_system_prompt(&record)?);
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
        record.install_system_prompt(self.effective_system_prompt(&record)?);
        let runtime = SessionRuntime::new(record);
        let mut sessions = self.sessions.lock().await;
        Ok(sessions.entry(id.to_string()).or_insert(runtime).clone())
    }

    /// Finds a session without loading it, unlike [`Self::get`].
    ///
    /// A stored session is only checked for a record file, not read, so an
    /// unreadable record is reported as inactive and fails when resumed.
    pub async fn presence(&self, id: &str) -> Result<SessionPresence, Error> {
        if validate_session_id(id).is_err() {
            return Ok(SessionPresence::Unknown);
        }
        if let Some(runtime) = self.sessions.lock().await.get(id).cloned() {
            return Ok(SessionPresence::Active(runtime));
        }
        if self.deleted.lock().await.contains(id) {
            return Ok(SessionPresence::Unknown);
        }
        match tokio::fs::metadata(self.record_path(id)).await {
            Ok(metadata) if metadata.is_file() => Ok(SessionPresence::Inactive),
            Ok(_) => Ok(SessionPresence::Unknown),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(SessionPresence::Unknown)
            }
            Err(error) => Err(Error::into_internal_error(error)),
        }
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
        self.forget_url_elicitations(id).await;
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
        self.forget_url_elicitations(id).await;
        Ok(())
    }

    /// Claim `elicitation_id` for a URL elicitation the session is about to
    /// request. ACP requires each ID to be unique among the connection's
    /// outstanding URL elicitations, so an ID still in use is refused. A
    /// request cancelled before its answer arrives is never settled: the
    /// Client may already have accepted it, so its ID stays reserved, and can
    /// never be completed, until the session closes.
    pub async fn reserve_url_elicitation(
        &self,
        session_id: &str,
        elicitation_id: &str,
    ) -> Result<(), Error> {
        let mut elicitations = self.url_elicitations.lock().await;
        // A session deleted while its turn still runs has already had its
        // elicitations dropped, so nothing would free a new reservation.
        // Close and delete remove the runtime before taking this lock.
        if !self.sessions.lock().await.contains_key(session_id) {
            return Err(Error::request_cancelled());
        }
        match elicitations.entry(elicitation_id.to_string()) {
            std::collections::hash_map::Entry::Occupied(_) => Err(Error::invalid_params().data(
                format!(
                    "elicitation ID {elicitation_id} is still in use by a URL elicitation that was not reported complete; omit elicitation_id to generate a new one"
                ),
            )),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(UrlElicitation {
                    session_id: session_id.to_string(),
                    accepted: false,
                });
                Ok(())
            }
        }
    }

    /// Record the Client's answer to a reserved URL elicitation. An accepted
    /// one stays outstanding until the model reports it complete or the
    /// session closes; any other answer, or an error instead of an answer,
    /// frees the ID.
    pub async fn settle_url_elicitation(
        &self,
        session_id: &str,
        elicitation_id: &str,
        accepted: bool,
    ) {
        let mut elicitations = self.url_elicitations.lock().await;
        let Some(elicitation) = elicitations
            .get_mut(elicitation_id)
            .filter(|elicitation| elicitation.session_id == session_id && !elicitation.accepted)
        else {
            return;
        };
        if accepted {
            elicitation.accepted = true;
        } else {
            elicitations.remove(elicitation_id);
        }
    }

    /// Take an accepted URL elicitation of the session so its completion can
    /// be reported. Unknown, unanswered, already completed, and other
    /// sessions' IDs return `false` and are left alone.
    pub async fn complete_url_elicitation(&self, session_id: &str, elicitation_id: &str) -> bool {
        let mut elicitations = self.url_elicitations.lock().await;
        let pending = elicitations.get(elicitation_id).is_some_and(|elicitation| {
            elicitation.session_id == session_id && elicitation.accepted
        });
        if pending {
            elicitations.remove(elicitation_id);
        }
        pending
    }

    /// Drop a closed or deleted session's URL elicitations without reporting
    /// them complete: closing says nothing about whether they finished.
    async fn forget_url_elicitations(&self, session_id: &str) {
        self.url_elicitations
            .lock()
            .await
            .retain(|_, elicitation| elicitation.session_id != session_id);
    }

    pub fn effective_system_prompt(&self, record: &SessionRecord) -> Result<String, Error> {
        // An override replaces only the harness's own text. Launch
        // configuration and AGENTS.md files still follow it.
        let (mut prompt, appended) = match record.client_system_prompt() {
            Some(ClientSystemPrompt {
                mode: ClientPromptMode::Override,
                mut text,
            }) => {
                if !text.ends_with('\n') {
                    text.push('\n');
                }
                (text, None)
            }
            client => (
                BUILT_IN_SYSTEM_PROMPT.to_string(),
                client.map(|prompt| prompt.text),
            ),
        };
        if let Some(extra) = &self.configured_prompt {
            prompt.push_str("\nAdditional user-configured instructions:\n");
            prompt.push_str(extra);
            prompt.push('\n');
        }
        let mut seen = std::collections::HashSet::new();
        let roots = std::iter::once(self.data_dir.as_path())
            .chain(std::iter::once(record.cwd.as_path()))
            .chain(record.additional_directories.iter().map(PathBuf::as_path))
            .filter(|root| seen.insert(*root));
        for root in roots {
            let path = root.join("AGENTS.md");
            let Some(instructions) = read_instructions(root, &path)? else {
                continue;
            };
            prompt.push_str("\nInstructions from ");
            prompt.push_str(&path.display().to_string());
            prompt.push_str(":\n");
            prompt.push_str(&instructions);
            if !instructions.ends_with('\n') {
                prompt.push('\n');
            }
        }
        if let Some(appended) = appended {
            prompt.push_str("\nClient-provided system instructions:\n");
            prompt.push_str(&appended);
            prompt.push('\n');
        }
        Ok(prompt)
    }

    /// Build the instructions a load or resume asks for without changing the
    /// session, so the request installs them only once its MCP servers
    /// connect.
    pub fn session_instructions(
        &self,
        record: &SessionRecord,
        additional_directories: Vec<PathBuf>,
        system_prompt: Option<ClientSystemPrompt>,
    ) -> Result<SessionInstructions, Error> {
        let mut candidate = record.clone();
        candidate.additional_directories = additional_directories;
        if let Some(system_prompt) = system_prompt.clone() {
            candidate.set_client_system_prompt(system_prompt);
        }
        let prompt = self.effective_system_prompt(&candidate)?;
        Ok(SessionInstructions {
            additional_directories: candidate.additional_directories,
            system_prompt,
            prompt,
        })
    }

    pub fn rebuild_system_prompt(&self, record: &mut SessionRecord) -> Result<(), Error> {
        let mut candidate = record.clone();
        let prompt = self.effective_system_prompt(&candidate)?;
        candidate.install_system_prompt(prompt);
        candidate.touch();
        *record = candidate;
        Ok(())
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

/// The workspace roots, client system prompt, and system message a load or
/// resume asked for, built by [`SessionStore::session_instructions`].
#[derive(Debug)]
pub struct SessionInstructions {
    additional_directories: Vec<PathBuf>,
    /// `None` keeps the session's stored client prompt.
    system_prompt: Option<ClientSystemPrompt>,
    prompt: String,
}

impl SessionInstructions {
    /// Install the instructions without touching the conversation, which a
    /// v2 turn may still be extending.
    pub fn install(self, record: &mut SessionRecord) {
        record.additional_directories = self.additional_directories;
        if let Some(system_prompt) = self.system_prompt {
            record.set_client_system_prompt(system_prompt);
        }
        record.install_system_prompt(self.prompt);
        record.touch();
    }
}

fn read_instructions(root: &Path, path: &Path) -> Result<Option<String>, Error> {
    let root = root.canonicalize().map_err(|error| {
        Error::internal_error().data(format!(
            "failed to resolve instruction root {}: {error}",
            root.display()
        ))
    })?;
    let target = match path.canonicalize() {
        Ok(target) => target,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::internal_error().data(format!(
                "failed to resolve instructions from {}: {error}",
                path.display()
            )));
        }
    };
    if !target.starts_with(&root) {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} resolve outside allowed root {}",
            path.display(),
            root.display()
        )));
    }
    let metadata = std::fs::metadata(&target).map_err(|error| {
        Error::internal_error().data(format!(
            "failed to inspect instructions from {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} are not a regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_INSTRUCTION_BYTES {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} exceed the {} byte limit",
            path.display(),
            MAX_INSTRUCTION_BYTES
        )));
    }
    let file = std::fs::File::open(&target).map_err(|error| {
        Error::internal_error().data(format!(
            "failed to open instructions from {}: {error}",
            path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        Error::internal_error().data(format!(
            "failed to inspect opened instructions from {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} changed to a non-regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_INSTRUCTION_BYTES {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} exceed the {} byte limit",
            path.display(),
            MAX_INSTRUCTION_BYTES
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_INSTRUCTION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            Error::internal_error().data(format!(
                "failed to read instructions from {}: {error}",
                path.display()
            ))
        })?;
    if bytes.len() as u64 > MAX_INSTRUCTION_BYTES {
        return Err(Error::invalid_params().data(format!(
            "instructions from {} exceed the {} byte limit",
            path.display(),
            MAX_INSTRUCTION_BYTES
        )));
    }
    let instructions = String::from_utf8(bytes).map_err(|_| {
        Error::invalid_params().data(format!(
            "instructions from {} are not valid UTF-8",
            path.display()
        ))
    })?;
    Ok((!instructions.trim().is_empty()).then_some(instructions))
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
            mcp_connect_timeout: std::time::Duration::from_secs(1),
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

    #[tokio::test]
    async fn presence_finds_stored_sessions_without_loading_them() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir: temp.path().join("data"),
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(temp.path().to_path_buf(), Vec::new(), None, None)
            .await
            .unwrap();
        let id = runtime.snapshot().await.id;
        assert!(matches!(
            store.presence(&id).await.unwrap(),
            SessionPresence::Active(_)
        ));

        // A second store over the same directory stands in for a restart.
        let restarted = SessionStore::open(&config).await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                restarted.presence(&id).await.unwrap(),
                SessionPresence::Inactive
            ));
        }
        assert!(restarted.sessions.lock().await.is_empty());

        store.close(&id).await.unwrap();
        assert!(matches!(
            store.presence(&id).await.unwrap(),
            SessionPresence::Inactive
        ));
        store.delete(&id).await.unwrap();
        for id in [id.as_str(), "dsh-unknown", "", "../../outside"] {
            assert!(
                matches!(store.presence(id).await.unwrap(), SessionPresence::Unknown),
                "{id:?}"
            );
        }
    }

    #[tokio::test]
    async fn url_elicitations_complete_once_and_only_for_their_session() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir: temp.path().join("data"),
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let mut sessions = Vec::new();
        for _ in 0..3 {
            let runtime = store
                .create(temp.path().to_path_buf(), Vec::new(), None, None)
                .await
                .unwrap();
            sessions.push(runtime.snapshot().await.id);
        }
        let [a, b, c] = [&sessions[0], &sessions[1], &sessions[2]];

        // An ID is unique on the connection while it is outstanding, even
        // across sessions, and an unanswered request cannot be completed.
        assert!(store.reserve_url_elicitation(a, "oauth").await.is_ok());
        let reused = store.reserve_url_elicitation(b, "oauth").await.unwrap_err();
        assert_eq!(reused.code, agent_client_protocol::ErrorCode::InvalidParams);
        assert!(!store.complete_url_elicitation(a, "oauth").await);

        // Declining frees the ID.
        store.settle_url_elicitation(a, "oauth", false).await;
        assert!(store.reserve_url_elicitation(b, "oauth").await.is_ok());
        store.settle_url_elicitation(b, "oauth", true).await;

        // Only the session that created it completes it, and only once.
        assert!(!store.complete_url_elicitation(a, "oauth").await);
        assert!(store.complete_url_elicitation(b, "oauth").await);
        assert!(!store.complete_url_elicitation(b, "oauth").await);
        assert!(store.reserve_url_elicitation(a, "oauth").await.is_ok());

        // Closing a session drops its accepted elicitations unreported.
        store.settle_url_elicitation(a, "oauth", true).await;
        store.close(a).await.unwrap();
        assert!(!store.complete_url_elicitation(a, "oauth").await);
        assert!(store.reserve_url_elicitation(b, "oauth").await.is_ok());

        // A turn still running after its session was deleted cannot reserve an
        // ID that nothing would free.
        store.delete(c).await.unwrap();
        let deleted = store
            .reserve_url_elicitation(c, "oauth-deleted")
            .await
            .unwrap_err();
        assert_eq!(
            deleted.code,
            agent_client_protocol::ErrorCode::RequestCancelled
        );
        assert!(
            store
                .reserve_url_elicitation(b, "oauth-deleted")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn loads_and_refreshes_agents_files_without_changing_client_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let workspace = temp.path().join("workspace");
        let extra = temp.path().join("extra");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&extra).unwrap();
        std::fs::write(data_dir.join("AGENTS.md"), "user-level instructions\n").unwrap();
        std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v1\n").unwrap();
        std::fs::write(extra.join("AGENTS.md"), "additional-root instructions\n").unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir: data_dir.clone(),
            permission_mode: PermissionMode::Ask,
            system_prompt: Some("launch instructions".to_string()),
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(
                workspace.clone(),
                vec![extra],
                Some(ClientSystemPrompt {
                    mode: ClientPromptMode::Append,
                    text: "client instructions".to_string(),
                }),
                None,
            )
            .await
            .unwrap();
        let record = runtime.snapshot().await;
        let prompt = record.messages[0].text_content().unwrap();
        let positions = [
            prompt.find("launch instructions").unwrap(),
            prompt.find("user-level instructions").unwrap(),
            prompt.find("workspace instructions v1").unwrap(),
            prompt.find("additional-root instructions").unwrap(),
            prompt.find("client instructions").unwrap(),
        ];
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            !record
                .system_prompt
                .as_deref()
                .unwrap()
                .contains("workspace")
        );

        std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v2\n").unwrap();
        let refreshed = store.effective_system_prompt(&record).unwrap();
        assert!(refreshed.contains("workspace instructions v2"));
        assert!(!refreshed.contains("workspace instructions v1"));
    }

    #[tokio::test]
    async fn an_override_replaces_only_the_built_in_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(data_dir.join("AGENTS.md"), "user-level instructions\n").unwrap();
        std::fs::write(workspace.join("AGENTS.md"), "workspace instructions\n").unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir,
            permission_mode: PermissionMode::Ask,
            system_prompt: Some("launch instructions".to_string()),
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(
                workspace,
                Vec::new(),
                Some(ClientSystemPrompt {
                    mode: ClientPromptMode::Override,
                    text: "client persona".to_string(),
                }),
                None,
            )
            .await
            .unwrap();
        let record = runtime.snapshot().await;
        let prompt = record.messages[0].text_content().unwrap();
        assert!(prompt.starts_with("client persona\n"), "{prompt}");
        assert!(!prompt.contains(BUILT_IN_SYSTEM_PROMPT), "{prompt}");
        assert!(!prompt.contains("Client-provided system instructions"));
        let positions = [
            prompt.find("launch instructions").unwrap(),
            prompt.find("user-level instructions").unwrap(),
            prompt.find("workspace instructions").unwrap(),
        ];
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));

        let mut appended = record.clone();
        store
            .session_instructions(
                &record,
                Vec::new(),
                Some(ClientSystemPrompt {
                    mode: ClientPromptMode::Append,
                    text: "client persona".to_string(),
                }),
            )
            .unwrap()
            .install(&mut appended);
        assert_eq!(appended.system_prompt_mode, ClientPromptMode::Append);
        let prompt = appended.messages[0].text_content().unwrap();
        assert!(prompt.starts_with(BUILT_IN_SYSTEM_PROMPT), "{prompt}");
        assert!(prompt.ends_with("Client-provided system instructions:\nclient persona\n"));
    }

    #[test]
    fn a_stored_client_prompt_without_a_mode_appends() {
        let record: SessionRecord = serde_json::from_value(serde_json::json!({
            "id": "dsh-1",
            "cwd": "/tmp",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "config": {
                "model": "deepseek-flash",
                "mode": "agent",
                "permission_mode": "ask",
                "network": true
            },
            "system_prompt": "client instructions"
        }))
        .unwrap();
        assert_eq!(
            record.client_system_prompt(),
            Some(ClientSystemPrompt {
                mode: ClientPromptMode::Append,
                text: "client instructions".to_string(),
            })
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn instruction_symlinks_cannot_escape_the_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let outside = temp.path().join("secret.txt");
        std::fs::write(&outside, "outside secret\n").unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir,
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(workspace, Vec::new(), None, None)
            .await
            .unwrap();
        let workspace = runtime.snapshot().await.cwd;
        std::os::unix::fs::symlink(outside, workspace.join("AGENTS.md")).unwrap();
        let record = runtime.snapshot().await;
        assert!(store.effective_system_prompt(&record).is_err());
    }

    #[tokio::test]
    async fn instruction_files_are_size_limited() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir,
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(workspace, Vec::new(), None, None)
            .await
            .unwrap();
        std::fs::write(
            runtime.snapshot().await.cwd.join("AGENTS.md"),
            vec![b'x'; MAX_INSTRUCTION_BYTES as usize + 1],
        )
        .unwrap();
        let record = runtime.snapshot().await;
        assert!(store.effective_system_prompt(&record).is_err());
    }

    #[tokio::test]
    async fn unreadable_instructions_fail_the_refresh() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let workspace = temp.path().join("workspace");
        let extra = temp.path().join("extra");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&extra).unwrap();
        let config = Config {
            api_key: None,
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir,
            permission_mode: PermissionMode::Ask,
            system_prompt: None,
            max_tokens: None,
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        };
        let store = SessionStore::open(&config).await.unwrap();
        let runtime = store
            .create(
                workspace.clone(),
                Vec::new(),
                Some(ClientSystemPrompt {
                    mode: ClientPromptMode::Append,
                    text: "client instructions v1".to_string(),
                }),
                None,
            )
            .await
            .unwrap();
        let record = runtime.snapshot().await;
        std::fs::write(workspace.join("AGENTS.md"), [0xff, 0xfe]).unwrap();

        assert!(
            store
                .session_instructions(
                    &record,
                    vec![extra],
                    Some(ClientSystemPrompt {
                        mode: ClientPromptMode::Override,
                        text: "client instructions v2".to_string(),
                    }),
                )
                .is_err()
        );
    }
}
