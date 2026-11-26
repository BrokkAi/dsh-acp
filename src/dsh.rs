//! Private runtime protocol between the Rust ACP server and the pinned DSH
//! runtime plugin.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use agent_client_protocol::{Error, ErrorCode};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, OnceCell, broadcast, oneshot},
};

const DSH_PACKAGE: &str = "@deepseek-ai/dsh";
pub const PINNED_DSH_VERSION: &str = "0.2.2-rc.5";
const PLUGIN_SOURCE: &str = include_str!("../dsh-plugin/index.mjs");

/// Launch configuration for the pinned DSH runtime.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub command: PathBuf,
    pub prefix_args: Vec<String>,
    pub profile: String,
    pub home: Option<PathBuf>,
    pub runtime_modules: Option<PathBuf>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub data_dir: PathBuf,
    pub request_timeout: Duration,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.command.as_os_str().is_empty() {
            return Err(Error::invalid_params().data("DSH command must not be empty"));
        }
        if self.profile.trim().is_empty() {
            return Err(Error::invalid_params().data("DSH profile must not be empty"));
        }
        if self.request_timeout.is_zero() {
            return Err(Error::invalid_params().data("DSH request timeout must be positive"));
        }
        Ok(())
    }
}

/// Requests awaiting a response. After the runtime's stdout closes no response
/// can arrive, so `closed` holds the failure for every later request.
#[derive(Default)]
struct Pending {
    replies: HashMap<u64, oneshot::Sender<Result<Value, Error>>>,
    closed: Option<Error>,
}

/// Removes a request's reply slot when its future finishes or is dropped, so an
/// abandoned request cannot leave an entry behind.
struct PendingGuard<'a> {
    pending: &'a StdMutex<Pending>,
    id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.replies.remove(&self.id);
        }
    }
}

struct RuntimeInner {
    stdin: Mutex<Option<BufWriter<ChildStdin>>>,
    pending: StdMutex<Pending>,
    notifications: broadcast::Sender<Value>,
    requests: broadcast::Sender<Value>,
    child: Mutex<Child>,
    next_id: AtomicU64,
    request_timeout: Duration,
}

/// One pinned DSH process and its private JSON-RPC transport.
#[derive(Clone)]
pub struct DshRuntime {
    inner: Arc<RuntimeInner>,
}

/// Lazily starts one DSH process per ACP connection.
#[derive(Clone)]
pub struct DshRuntimeProvider {
    config: RuntimeConfig,
    runtime: Arc<OnceCell<Result<Arc<DshRuntime>, String>>>,
}

impl DshRuntimeProvider {
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            runtime: Arc::new(OnceCell::new()),
        }
    }

    pub async fn get(&self) -> Result<Arc<DshRuntime>, Error> {
        self.runtime
            .get_or_init(|| async {
                DshRuntime::start(self.config.clone())
                    .await
                    .map(Arc::new)
                    .map_err(|error| format!("{error:?}"))
            })
            .await
            .clone()
            .map_err(|error| {
                Error::internal_error().data(format!(
                    "pinned DeepSeek Harness runtime is unavailable: {error}"
                ))
            })
    }

    pub async fn close(&self) {
        if let Some(Ok(runtime)) = self.runtime.get() {
            runtime.close().await;
        }
    }
}

impl Drop for DshRuntimeProvider {
    fn drop(&mut self) {
        if Arc::strong_count(&self.runtime) != 1 {
            return;
        }
        let Some(Ok(runtime)) = self.runtime.get() else {
            return;
        };
        let runtime = Arc::clone(runtime);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                runtime.close().await;
            });
        }
    }
}

impl DshRuntime {
    pub async fn start(config: RuntimeConfig) -> Result<Self, Error> {
        config.validate()?;
        let patch = install_runtime_assets(&config.data_dir)?;
        let mut command = Command::new(&config.command);
        command
            .args(&config.prefix_args)
            .arg("--profile")
            .arg(&config.profile)
            .arg("--patch")
            .arg(&patch)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(home) = &config.home {
            command.env("DSH_HOME", home);
        }
        if let Some(modules) = config
            .runtime_modules
            .clone()
            .or_else(|| infer_runtime_modules(&config))
        {
            command.env("DSH_ACP_RUNTIME_MODULES", modules);
        }
        if let Some(api_key) = &config.api_key {
            command.env("DEEPSEEK_API_KEY", api_key);
        }
        if let Some(base_url) = &config.base_url {
            command.env("DEEPSEEK_BASE_URL", base_url);
        }
        let mut child = command.spawn().map_err(|error| {
            Error::internal_error().data(format!(
                "failed to start pinned {DSH_PACKAGE} {PINNED_DSH_VERSION}: {error}"
            ))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::internal_error().data("DSH runtime has no stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::internal_error().data("DSH runtime has no stdout pipe"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::internal_error().data("DSH runtime has no stderr pipe"))?;
        let (notifications, _) = broadcast::channel(1024);
        let (requests, _) = broadcast::channel(64);
        let runtime = Self {
            inner: Arc::new(RuntimeInner {
                stdin: Mutex::new(Some(BufWriter::new(stdin))),
                pending: StdMutex::new(Pending::default()),
                notifications,
                requests,
                child: Mutex::new(child),
                next_id: AtomicU64::new(1),
                request_timeout: config.request_timeout,
            }),
        };
        tokio::spawn(read_stdout(Arc::clone(&runtime.inner), stdout));
        tokio::spawn(read_stderr(stderr));
        let hello = runtime.request("dsh/hello", json!({})).await?;
        let version = hello
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if version != PINNED_DSH_VERSION {
            runtime.close().await;
            return Err(Error::internal_error().data(format!(
                "DeepSeek Harness {PINNED_DSH_VERSION} is required, but the runtime reported {version:?}"
            )));
        }
        Ok(runtime)
    }

    /// Sends a request that the runtime must answer within `request_timeout`.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        self.send_request(method, params, Some(self.inner.request_timeout))
            .await
    }

    /// Waits until DSH finishes everything it is doing for `session_id`.
    ///
    /// DSH answers `session/wait` only when the whole turn ends, and a turn may
    /// legitimately run for any length of time, so this has no deadline. It
    /// still fails when the runtime exits: a closed runtime answers every
    /// pending and later request with an error. Dropping the future abandons
    /// the wait.
    pub async fn wait_session(&self, session_id: String) -> Result<Value, Error> {
        self.send_request("session/wait", json!({"sessionId": session_id}), None)
            .await
    }

    async fn send_request(
        &self,
        method: &str,
        params: Value,
        deadline: Option<Duration>,
    ) -> Result<Value, Error> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, response) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
            if let Some(error) = &pending.closed {
                return Err(error.clone());
            }
            pending.replies.insert(id, reply);
        }
        let _guard = PendingGuard {
            pending: &self.inner.pending,
            id,
        };
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.write_frame(frame).await?;
        let response = async {
            response.await.unwrap_or_else(|_| {
                Err(Error::internal_error().data("DSH runtime closed the request"))
            })
        };
        match deadline {
            None => response.await,
            Some(deadline) => tokio::time::timeout(deadline, response)
                .await
                .unwrap_or_else(|_| {
                    Err(Error::internal_error().data(format!(
                        "DSH runtime did not answer {method} within {deadline:?}"
                    )))
                }),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.notifications.subscribe()
    }

    pub fn subscribe_requests(&self) -> broadcast::Receiver<Value> {
        self.inner.requests.subscribe()
    }

    pub async fn respond(&self, id: Value, result: Value) -> Result<(), Error> {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        self.write_frame(frame).await
    }

    pub async fn respond_error(&self, id: Value, message: impl Into<String>) -> Result<(), Error> {
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32603, "message": message.into()},
        });
        self.write_frame(frame).await
    }

    async fn write_frame(&self, frame: Value) -> Result<(), Error> {
        let mut stdin = self.inner.stdin.lock().await;
        let stdin = stdin
            .as_mut()
            .ok_or_else(|| Error::internal_error().data("DSH runtime is closed"))?;
        stdin
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .map_err(Error::into_internal_error)?;
        stdin.flush().await.map_err(Error::into_internal_error)
    }

    pub async fn close(&self) {
        self.inner.stdin.lock().await.take();
        let mut child = self.inner.child.lock().await;
        if tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .is_err()
        {
            let _ = child.kill().await;
        }
    }
}

fn infer_runtime_modules(config: &RuntimeConfig) -> Option<PathBuf> {
    let mut candidates = vec![config.command.clone()];
    candidates.extend(
        config
            .prefix_args
            .iter()
            .filter(|argument| argument.ends_with(".js") || argument.ends_with(".mjs"))
            .map(PathBuf::from),
    );
    for candidate in candidates {
        let resolved = std::fs::canonicalize(&candidate).unwrap_or(candidate);
        if resolved.file_name().is_some_and(|name| name == "bin.js")
            && let Some(modules) = resolved
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent)
                .and_then(Path::parent)
        {
            return Some(modules.to_path_buf());
        }
    }
    None
}

fn install_runtime_assets(data_dir: &Path) -> Result<PathBuf, Error> {
    let directory = data_dir.join("dsh-runtime");
    std::fs::create_dir_all(&directory).map_err(Error::into_internal_error)?;
    let plugin = directory.join("index.mjs");
    std::fs::write(&plugin, PLUGIN_SOURCE).map_err(Error::into_internal_error)?;
    let patch = directory.join("profile.patch.yml");
    let plugin_url = format!("file://{}", plugin.display());
    std::fs::write(
        &patch,
        format!(
            "- id: sdk-jsonrpc-server\n  disabled: true\n\n- insert:\n    - id: dsh-acp-runtime\n      name: '{plugin_url}'\n"
        ),
    )
    .map_err(Error::into_internal_error)?;
    Ok(patch)
}

async fn read_stdout(inner: Arc<RuntimeInner>, stdout: tokio::process::ChildStdout) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            break;
        };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            tracing::warn!(line, "ignored non-JSON DSH runtime output");
            continue;
        };
        let response_id = message.get("id").and_then(Value::as_u64);
        let is_response = message.get("result").is_some() || message.get("error").is_some();
        if is_response {
            if let Some(id) = response_id {
                let reply = inner
                    .pending
                    .lock()
                    .expect("pending lock poisoned")
                    .replies
                    .remove(&id);
                if let Some(reply) = reply {
                    let result = if let Some(error) = message.get("error") {
                        Err(Error::new(
                            ErrorCode::INTERNAL_ERROR,
                            error
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or("DSH runtime request failed"),
                        ))
                    } else {
                        Ok(message.get("result").cloned().unwrap_or(Value::Null))
                    };
                    let _ = reply.send(result);
                }
            }
            continue;
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            let _ = inner.requests.send(message);
            continue;
        }
        let _ = inner.notifications.send(message);
    }
    let error = Error::internal_error().data("DSH runtime protocol stdout closed");
    let replies = {
        let mut pending = inner.pending.lock().expect("pending lock poisoned");
        pending.closed = Some(error.clone());
        std::mem::take(&mut pending.replies)
    };
    for (_, reply) in replies {
        let _ = reply.send(Err(error.clone()));
    }
}

async fn read_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "dsh_runtime", "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_patch_replaces_only_the_sdk_protocol_entry() {
        let temp = tempfile::tempdir().unwrap();
        let patch = install_runtime_assets(temp.path()).unwrap();
        let text = std::fs::read_to_string(patch).unwrap();
        assert!(text.contains("- id: sdk-jsonrpc-server\n  disabled: true"));
        assert!(text.contains("- id: dsh-acp-runtime"));
        assert!(text.contains("file://"));
    }

    #[test]
    fn npm_and_plugin_manifests_pin_the_same_runtime() {
        let npm: Value = serde_json::from_str(include_str!("../npm/package.json")).unwrap();
        assert_eq!(npm["dependencies"]["@deepseek-ai/dsh"], PINNED_DSH_VERSION);
        let plugin: Value =
            serde_json::from_str(include_str!("../dsh-plugin/package.json")).unwrap();
        assert_eq!(plugin["engines"]["dsh"], PINNED_DSH_VERSION);
    }

    #[test]
    fn runtime_modules_are_inferred_from_the_dsh_javascript_entrypoint() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("node_modules/@deepseek-ai/dsh/lib/bin.js");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, "").unwrap();
        let config = RuntimeConfig {
            command: PathBuf::from("node"),
            prefix_args: vec![bin.display().to_string()],
            profile: "sdk".to_string(),
            home: None,
            runtime_modules: None,
            api_key: None,
            base_url: None,
            data_dir: temp.path().join("data"),
            request_timeout: Duration::from_secs(1),
        };
        let modules = std::fs::canonicalize(temp.path().join("node_modules")).unwrap();
        assert_eq!(infer_runtime_modules(&config), Some(modules));
    }

    /// A stand-in for the DSH runtime. `dsh/hello` reports the pinned version,
    /// `dsh/exit` ends the process without replying, and every other request is
    /// answered after the delay named by its `delayMs` parameter or by a
    /// `wait-<ms>` session id.
    const FAKE_RUNTIME: &str = r#"
const readline = require('node:readline')
const reply = (id, result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n')
readline.createInterface({ input: process.stdin }).on('close', () => process.exit(0)).on('line', line => {
  const { id, method, params } = JSON.parse(line)
  if (method === 'dsh/hello') return reply(id, { version: 'PINNED_VERSION' })
  if (method === 'dsh/exit') process.exit(3)
  const named = Number((params.sessionId ?? '').split('-')[1])
  setTimeout(() => reply(id, {}), params.delayMs ?? (named || 0))
})
"#;

    async fn start_fake_runtime(directory: &Path, request_timeout: Duration) -> DshRuntime {
        let script = directory.join("fake-runtime.cjs");
        std::fs::write(
            &script,
            FAKE_RUNTIME.replace("PINNED_VERSION", PINNED_DSH_VERSION),
        )
        .unwrap();
        DshRuntime::start(RuntimeConfig {
            command: std::env::var_os("DSH_ACP_TEST_NODE")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("node")),
            prefix_args: vec![script.display().to_string()],
            profile: "sdk".to_string(),
            home: Some(directory.join("home")),
            runtime_modules: None,
            api_key: None,
            base_url: None,
            data_dir: directory.join("data"),
            request_timeout,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn session_wait_outlasts_the_request_timeout() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = start_fake_runtime(temp.path(), Duration::from_millis(500)).await;
        // The turn takes four times longer than the request timeout.
        runtime.wait_session("wait-2000".to_string()).await.unwrap();
        // Other requests still have the deadline.
        let slow = runtime
            .request("dsh/slow", json!({"delayMs": 2000}))
            .await
            .unwrap_err();
        assert!(
            format!("{slow:?}").contains("did not answer dsh/slow within 500ms"),
            "{slow:?}"
        );
        runtime.close().await;
    }

    #[tokio::test]
    async fn runtime_exit_fails_a_pending_wait_and_later_requests() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = start_fake_runtime(temp.path(), Duration::from_secs(60)).await;
        let wait = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.wait_session("wait-600000".to_string()).await }
        });
        // Let the wait reach the runtime before it exits.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let exit = runtime.request("dsh/exit", json!({})).await.unwrap_err();
        assert!(format!("{exit:?}").contains("stdout closed"), "{exit:?}");
        let wait = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("a wait must not outlive the runtime")
            .unwrap()
            .unwrap_err();
        assert!(format!("{wait:?}").contains("stdout closed"), "{wait:?}");
        let later = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.wait_session("wait-600000".to_string()),
        )
        .await
        .expect("a wait on a dead runtime must not hang")
        .unwrap_err();
        assert!(format!("{later:?}").contains("stdout closed"), "{later:?}");
    }

    #[tokio::test]
    async fn dropping_a_wait_releases_its_pending_entry() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = start_fake_runtime(temp.path(), Duration::from_secs(60)).await;
        let abandoned = tokio::time::timeout(
            Duration::from_millis(200),
            runtime.wait_session("wait-600000".to_string()),
        )
        .await;
        assert!(abandoned.is_err());
        assert!(runtime.inner.pending.lock().unwrap().replies.is_empty());
        runtime.close().await;
    }
}
