use std::process::Stdio;

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Peer {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    updates: Vec<Value>,
    permission_requests: Vec<Value>,
    permission_answer: &'static str,
    /// Each `terminal/*` request the agent sent, with how many updates the
    /// peer had received by then.
    terminal_requests: Vec<(String, usize)>,
    /// Answer `terminal/wait_for_exit` with an error.
    fail_terminal_wait: bool,
}

/// The id and output of the terminal the peer pretends to run.
const CLIENT_TERMINAL_ID: &str = "client-term-1";
const CLIENT_TERMINAL_OUTPUT: &str = "client terminal output\n";

impl Peer {
    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await;
        loop {
            let value = self.next_message().await;
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = value.get("error") {
                    panic!("{method} failed: {error}");
                }
                return value.get("result").cloned().unwrap_or(Value::Null);
            }
            assert_eq!(
                value.get("method").and_then(Value::as_str),
                Some("session/update")
            );
            self.updates.push(value);
        }
    }

    async fn request_error(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await;
        loop {
            let value = self.next_message().await;
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                return value
                    .get("error")
                    .cloned()
                    .expect("expected an error response");
            }
            self.updates.push(value);
        }
    }

    async fn next_message(&mut self) -> Value {
        loop {
            let mut line = String::new();
            let read = self.stdout.read_line(&mut line).await.unwrap();
            assert!(read > 0, "server closed stdout before replying");
            let value: Value = serde_json::from_str(&line).expect("valid JSON-RPC");
            if value.get("method").and_then(Value::as_str) == Some("fs/read_text_file") {
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": value["id"],
                    "result": {"content": "sample file contents\n"}
                }))
                .await;
                continue;
            }
            if value.get("method").and_then(Value::as_str) == Some("fs/write_text_file") {
                std::fs::write(
                    value["params"]["path"].as_str().unwrap(),
                    value["params"]["content"].as_str().unwrap(),
                )
                .unwrap();
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": value["id"],
                    "result": {}
                }))
                .await;
                continue;
            }
            if let Some(method) = value.get("method").and_then(Value::as_str)
                && method.starts_with("terminal/")
            {
                self.terminal_requests
                    .push((method.to_string(), self.updates.len()));
                let reply = match method {
                    "terminal/create" => json!({"result": {"terminalId": CLIENT_TERMINAL_ID}}),
                    "terminal/wait_for_exit" if self.fail_terminal_wait => {
                        json!({"error": {"code": -32603, "message": "terminal lost"}})
                    }
                    "terminal/wait_for_exit" => json!({"result": {"exitCode": 0}}),
                    "terminal/output" => json!({"result": {
                        "output": CLIENT_TERMINAL_OUTPUT,
                        "truncated": false,
                        "exitStatus": {"exitCode": 0}
                    }}),
                    _ => json!({"result": {}}),
                };
                let mut reply = reply.as_object().unwrap().clone();
                reply.insert("jsonrpc".to_string(), json!("2.0"));
                reply.insert("id".to_string(), value["id"].clone());
                self.write(Value::Object(reply)).await;
                continue;
            }
            if value.get("method").and_then(Value::as_str) == Some("session/request_permission") {
                self.permission_requests.push(value.clone());
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": value["id"],
                    "result": {
                        "outcome": {
                            "outcome": "selected",
                            "optionId": self.permission_answer
                        }
                    }
                }))
                .await;
                continue;
            }
            return value;
        }
    }

    async fn write(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        self.stdin.write_all(&bytes).await.unwrap();
        self.stdin.flush().await.unwrap();
    }
}

#[tokio::test]
async fn a_request_asks_for_the_models_real_output_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_deepseek_server().await else {
        eprintln!("skipping output allowance test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let _ = peer_prompt(&mut peer, temp.path(), "Write sample.txt and report back").await;

    let requests = server.await.unwrap();
    // DeepSeek's own examples do not send `max_tokens`, letting the API use
    // its maximum (384000). Capping it lower truncates a large file write or a
    // long chain of thought into `finish_reason: "length"`.
    let request = &requests[0];
    match request.get("max_tokens") {
        None => {}
        Some(value) => {
            let max_tokens = value.as_u64().unwrap();
            assert!(
                max_tokens >= 65_536,
                "max_tokens {max_tokens} is far below what the model allows"
            );
            assert!(
                max_tokens <= 384_000,
                "max_tokens {max_tokens} exceeds the API"
            );
        }
    }
}

#[tokio::test]
async fn a_prompt_is_not_capped_at_a_fixed_number_of_tool_rounds() {
    let temp = tempfile::tempdir().unwrap();
    // Comfortably more rounds than the 24-round cap the harness used to
    // impose, so a reintroduced cap fails this test.
    const ROUNDS: usize = 30;
    let Some((base_url, server)) = mock_long_tool_loop_server(ROUNDS).await else {
        eprintln!("skipping round-limit test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {}, "terminal": false}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();

    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Take as many steps as you need"}]
            }),
        )
        .await;

    assert_eq!(
        response["stopReason"], "end_turn",
        "a long turn must end because the model finished, not at a round cap"
    );
    let executed = peer
        .updates
        .iter()
        .filter(|update| {
            let update = &update["params"]["update"];
            update["sessionUpdate"] == "tool_call_update"
                && update["status"] == "completed"
                && update["toolCallId"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("call-"))
        })
        .count();
    assert_eq!(executed, ROUNDS, "every tool call of the turn should run");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && update["params"]["update"]["content"]["text"] == "mock answer"
    }));
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), ROUNDS + 1);
}

#[tokio::test]
async fn stdio_supports_initialize_and_session_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer = spawn_peer(temp.path(), None).await;

    let initialize = peer
        .request(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": {"readTextFile": true, "writeTextFile": true},
                    "terminal": true
                }
            }),
        )
        .await;
    assert_eq!(initialize["protocolVersion"], 1);
    assert_eq!(initialize["authMethods"][0]["id"], "deepseek-api-key");
    assert_eq!(initialize["_meta"]["steering"]["supported"], true);
    assert!(
        initialize["_meta"]["steering"]["idleBehaviors"]
            .as_array()
            .unwrap()
            .contains(&json!("promptRequired"))
    );
    assert_eq!(
        initialize["agentCapabilities"]["sessionCapabilities"]["close"],
        json!({})
    );
    assert_eq!(
        initialize["agentCapabilities"]["mcpCapabilities"]["http"],
        true
    );

    let cwd = temp.path().to_string_lossy().to_string();
    let new_session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = new_session["sessionId"].as_str().unwrap().to_string();
    assert_eq!(new_session["modes"]["currentModeId"], "agent");
    assert!(new_session["configOptions"].as_array().unwrap().len() >= 3);
    let idle_steer = peer
        .request(
            "_session/steering",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Only steer a running turn"}],
                "_meta": {"steering": {"idleBehavior": "promptRequired"}}
            }),
        )
        .await;
    assert_eq!(idle_steer["outcome"], "promptRequired");
    assert_eq!(idle_steer["reason"], "noRunningTurn");

    let prompt = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "/help"}]
            }),
        )
        .await;
    assert_eq!(prompt["stopReason"], "end_turn");

    let auth_error = peer
        .request_error(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "This requires DeepSeek."}]
            }),
        )
        .await;
    assert_eq!(auth_error["code"], -32000);
    let after_error = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "/help"}]
            }),
        )
        .await;
    assert_eq!(after_error["stopReason"], "end_turn");

    let providers = peer.request("providers/list", json!({})).await;
    assert_eq!(providers["providers"][0]["providerId"], "deepseek");
    assert_eq!(providers["providers"][0]["current"]["apiType"], "openai");
    peer.request(
        "providers/set",
        json!({
            "providerId": "deepseek",
            "apiType": "openai",
            "baseUrl": "http://127.0.0.1:9",
            "headers": {}
        }),
    )
    .await;
    let providers = peer.request("providers/list", json!({})).await;
    assert_eq!(
        providers["providers"][0]["current"]["baseUrl"],
        "http://127.0.0.1:9"
    );
    peer.request("providers/disable", json!({"providerId": "deepseek"}))
        .await;
    let providers = peer.request("providers/list", json!({})).await;
    assert!(providers["providers"][0].get("current").is_none());
    let logout = peer.request("logout", json!({})).await;
    assert_eq!(logout, json!({}));

    let set_mode = peer
        .request(
            "session/set_mode",
            json!({"sessionId": session_id, "modeId": "plan"}),
        )
        .await;
    assert_eq!(set_mode, json!({}));

    let set_model = peer
        .request(
            "session/set_config_option",
            json!({
                "sessionId": session_id,
                "configId": "model",
                "value": "deepseek-flash"
            }),
        )
        .await;
    assert!(set_model["configOptions"].as_array().unwrap().len() >= 3);

    let list = peer.request("session/list", json!({"cwd": cwd})).await;
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(list["sessions"][0]["sessionId"], session_id);

    let fork = peer
        .request(
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    let fork_id = fork["sessionId"].as_str().unwrap().to_string();
    assert_ne!(fork_id, session_id);

    let resume = peer
        .request(
            "session/resume",
            json!({
                "sessionId": fork_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    assert!(resume["modes"].is_object());

    let load = peer
        .request(
            "session/load",
            json!({
                "sessionId": fork_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    assert!(load["modes"].is_object());

    let close = peer
        .request("session/close", json!({"sessionId": session_id}))
        .await;
    assert_eq!(close, json!({}));

    let delete = peer
        .request("session/delete", json!({"sessionId": fork_id}))
        .await;
    assert_eq!(delete, json!({}));
    let delete_again = peer
        .request("session/delete", json!({"sessionId": fork_id}))
        .await;
    assert_eq!(delete_again, json!({}));

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
}

#[tokio::test]
async fn stdio_negotiates_acp_v2_and_uses_async_prompt_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer = spawn_peer(temp.path(), None).await;
    let initialize = peer
        .request(
            "initialize",
            json!({
                "protocolVersion": 2,
                "info": {"name": "v2-test-client", "version": "1.0.0"},
                "capabilities": {}
            }),
        )
        .await;
    assert_eq!(initialize["protocolVersion"], 2);
    assert_eq!(initialize["authMethods"][0]["methodId"], "deepseek-api-key");
    assert_eq!(initialize["_meta"]["steering"]["supported"], true);
    assert!(
        initialize["_meta"]["steering"]["idleBehaviors"]
            .as_array()
            .unwrap()
            .contains(&json!("promptRequired"))
    );
    assert!(initialize["capabilities"]["session"].is_object());
    assert!(initialize["capabilities"]["providers"].is_object());
    let providers = peer.request("providers/list", json!({})).await;
    assert_eq!(providers["providers"][0]["providerId"], "deepseek");

    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    assert!(session["configOptions"].as_array().unwrap().len() >= 4);
    assert!(
        session["availableCommands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["name"] == "plan")
    );

    let accepted = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "/help"}]
            }),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());

    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("v2 prompt should settle to idle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");

    let extra = temp.path().join("extra-root");
    std::fs::create_dir(&extra).unwrap();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": cwd,
            "mcpServers": [],
            "additionalDirectories": [extra]
        }),
    )
    .await;
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": cwd,
            "mcpServers": [],
            "additionalDirectories": []
        }),
    )
    .await;
    let list = peer.request("session/list", json!({"cwd": cwd})).await;
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    assert!(
        list["sessions"][0]
            .get("additionalDirectories")
            .is_none_or(|directories| directories.as_array().is_some_and(Vec::is_empty))
    );

    let fork = peer
        .request(
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    let fork_id = fork["sessionId"].as_str().unwrap().to_string();
    let resumed = peer
        .request(
            "session/resume",
            json!({
                "sessionId": fork_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": [],
                "replayFrom": {"type": "start"}
            }),
        )
        .await;
    assert!(resumed["configOptions"].as_array().unwrap().len() >= 4);
    let config = peer
        .request(
            "session/set_config_option",
            json!({
                "sessionId": fork_id,
                "configId": "mode",
                "type": "id",
                "value": "plan"
            }),
        )
        .await;
    assert!(config["configOptions"].as_array().unwrap().len() >= 4);
    peer.request("session/delete", json!({"sessionId": fork_id}))
        .await;
    let deleted_again = peer
        .request("session/delete", json!({"sessionId": fork_id}))
        .await;
    assert_eq!(deleted_again, json!({}));

    let close = peer
        .request("session/close", json!({"sessionId": session_id}))
        .await;
    assert_eq!(close, json!({}));

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
}

async fn spawn_peer(data_dir: &std::path::Path, base_url: Option<&str>) -> Peer {
    spawn_peer_with_args(data_dir, base_url, &[]).await
}

async fn spawn_peer_with_args(
    data_dir: &std::path::Path,
    base_url: Option<&str>,
    extra_args: &[&str],
) -> Peer {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dsh-acp"));
    command
        .arg("--data-dir")
        .arg(data_dir.join("sessions-data"))
        .arg("--disable-web")
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(base_url) = base_url {
        command
            .arg("--base-url")
            .arg(base_url)
            .arg("--permission-mode")
            .arg("ask")
            .env("DEEPSEEK_API_KEY", "test-key");
    } else {
        command.env_remove("DEEPSEEK_API_KEY");
    }
    let mut child = command.spawn().unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    Peer {
        child,
        stdin,
        stdout,
        next_id: 1,
        updates: Vec::new(),
        permission_requests: Vec::new(),
        permission_answer: "allow_once",
        terminal_requests: Vec::new(),
        fail_terminal_wait: false,
    }
}

#[tokio::test]
async fn streams_text_and_executes_a_client_filesystem_tool() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("sample.txt"), "sample file contents\n").unwrap();
    let Some((base_url, server)) = mock_deepseek_server().await else {
        eprintln!("skipping streaming test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;

    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": {"readTextFile": true, "writeTextFile": true},
                "terminal": false
            }
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Write sample.txt and report back"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call"
            && update["params"]["update"]["name"] == "write_file"
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && update["params"]["update"]["content"]["text"] == "mock answer"
    }));
    assert_eq!(peer.permission_requests.len(), 1);
    assert_eq!(
        std::fs::read_to_string(temp.path().join("sample.txt")).unwrap(),
        "written by tool"
    );

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].to_string().contains("written by tool"));
}

#[tokio::test]
async fn command_permission_title_quotes_arguments_as_a_shell_would() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_command_server().await else {
        eprintln!("skipping command title test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": "run a command"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(peer.permission_requests.len(), 1);
    assert_eq!(
        peer.permission_requests[0]["params"]["toolCall"]["title"],
        "Run sh -c 'printf terminal-output'"
    );
    // Without a client terminal the command runs locally, and its result is
    // text as before.
    let finished = peer
        .updates
        .iter()
        .map(|update| &update["params"]["update"])
        .rfind(|update| update["toolCallId"] == "call-command")
        .unwrap();
    assert_eq!(finished["status"], "completed");
    assert_eq!(finished["content"].as_array().unwrap().len(), 1);
    assert_eq!(finished["content"][0]["type"], "content");
    assert!(
        finished["content"][0]["content"]["text"]
            .as_str()
            .unwrap()
            .contains("terminal-output")
    );
    assert!(peer.terminal_requests.is_empty());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 2);
}

/// One model turn that asks for a single tool call.
fn tool_call_stream(id: &str, name: &str, arguments: Value) -> String {
    encode_sse(&[
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":arguments.to_string()}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
    ])
}

#[tokio::test]
async fn edit_and_write_permission_requests_show_the_change() {
    let temp = tempfile::tempdir().unwrap();
    let cwd = temp.path().canonicalize().unwrap();
    std::fs::write(cwd.join("sample.txt"), "sample file contents\n").unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        tool_call_stream(
            "call-edit",
            "edit_file",
            json!({"path": "sample.txt", "old_text": "contents", "new_text": "changes"}),
        ),
        tool_call_stream(
            "call-write",
            "write_file",
            json!({"path": "new.txt", "content": "fresh\n"}),
        ),
        tool_call_stream(
            "call-missing",
            "edit_file",
            json!({"path": "sample.txt", "old_text": "absent", "new_text": "x"}),
        ),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping permission preview test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(&cwd, Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd.to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": "change the files"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");

    let edit_diff = json!([{
        "type": "diff",
        "path": cwd.join("sample.txt"),
        "oldText": "sample file contents\n",
        "newText": "sample file changes\n"
    }]);
    assert_eq!(peer.permission_requests.len(), 3);
    let tool_call = |index: usize| &peer.permission_requests[index]["params"]["toolCall"];
    assert_eq!(tool_call(0)["toolCallId"], "call-edit");
    assert_eq!(tool_call(0)["content"], edit_diff);
    assert_eq!(tool_call(1)["toolCallId"], "call-write");
    let write_diff = tool_call(1)["content"].as_array().unwrap();
    assert_eq!(write_diff.len(), 1);
    assert_eq!(write_diff[0]["type"], "diff");
    assert_eq!(write_diff[0]["path"], json!(cwd.join("new.txt")));
    assert!(write_diff[0]["oldText"].is_null(), "{write_diff:?}");
    assert_eq!(write_diff[0]["newText"], "fresh\n");
    // The edit cannot be applied, so the request stays as it was: no diff.
    assert_eq!(tool_call(2)["toolCallId"], "call-missing");
    assert!(tool_call(2).get("content").is_none(), "{}", tool_call(2));

    let pending = |id: &str| {
        peer.updates
            .iter()
            .map(|update| &update["params"]["update"])
            .find(|update| update["sessionUpdate"] == "tool_call" && update["toolCallId"] == id)
            .unwrap()
    };
    assert_eq!(pending("call-edit")["content"], edit_diff);
    assert!(pending("call-missing").get("content").is_none());
    assert!(peer.updates.iter().any(|update| {
        let update = &update["params"]["update"];
        update["toolCallId"] == "call-missing" && update["status"] == "failed"
    }));
    assert_eq!(
        std::fs::read_to_string(cwd.join("sample.txt")).unwrap(),
        "sample file changes\n"
    );
    assert_eq!(
        std::fs::read_to_string(cwd.join("new.txt")).unwrap(),
        "fresh\n"
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 4);
}

#[tokio::test]
async fn a_command_in_the_client_terminal_shows_that_terminal_in_its_tool_call() {
    let temp = tempfile::tempdir().unwrap();
    let command = json!({"command": "sh", "args": ["-c", "printf terminal-output"]});
    let Some((base_url, server)) = mock_sequence_server(vec![
        tool_call_stream("call-ok", "run_command", command.clone()),
        mock_text_stream(),
        tool_call_stream("call-broken", "run_command", command),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping client terminal test: loopback networking is unavailable");
        return;
    };
    let cwd = temp.path().to_string_lossy().to_string();
    let initialize = json!({"protocolVersion": 1, "clientCapabilities": {"terminal": true}});
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", initialize.clone()).await;
    let session = peer
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let prompt = json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "run it"}]});
    let response = peer.request("session/prompt", prompt.clone()).await;
    assert_eq!(response["stopReason"], "end_turn");

    let terminal = json!([{"type": "terminal", "terminalId": CLIENT_TERMINAL_ID}]);
    let updates_for = |updates: &[Value], id: &str| -> Vec<Value> {
        updates
            .iter()
            .map(|update| update["params"]["update"].clone())
            .filter(|update| update["toolCallId"] == id)
            .collect()
    };
    // The terminal joins the command's tool call once the client creates it,
    // before the agent releases it.
    let shown = peer
        .updates
        .iter()
        .position(|update| {
            let update = &update["params"]["update"];
            update["toolCallId"] == "call-ok"
                && update["status"] == "in_progress"
                && update["content"] == terminal
        })
        .expect("an update shows the terminal in the tool call");
    let methods: Vec<&str> = peer
        .terminal_requests
        .iter()
        .map(|(method, _)| method.as_str())
        .collect();
    assert_eq!(
        methods,
        [
            "terminal/create",
            "terminal/wait_for_exit",
            "terminal/output",
            "terminal/release"
        ]
    );
    assert!(shown < peer.terminal_requests[3].1, "shown after release");
    // The final update keeps the terminal and does not repeat its output as
    // text; rawOutput still carries the result.
    let ok = updates_for(&peer.updates, "call-ok");
    let finished = ok.last().unwrap();
    assert_eq!(finished["status"], "completed");
    assert_eq!(finished["content"], terminal);
    assert_eq!(finished["rawOutput"]["output"], CLIENT_TERMINAL_OUTPUT);
    assert_eq!(finished["rawOutput"]["exit_code"], 0);

    // A command that fails after the terminal exists keeps the terminal and
    // adds the error.
    peer.fail_terminal_wait = true;
    peer.terminal_requests.clear();
    let response = peer.request("session/prompt", prompt).await;
    assert_eq!(response["stopReason"], "end_turn");
    let methods: Vec<&str> = peer
        .terminal_requests
        .iter()
        .map(|(method, _)| method.as_str())
        .collect();
    assert_eq!(
        methods,
        [
            "terminal/create",
            "terminal/wait_for_exit",
            "terminal/kill",
            "terminal/release"
        ]
    );
    let broken = updates_for(&peer.updates, "call-broken");
    let finished = broken.last().unwrap();
    assert_eq!(finished["status"], "failed");
    let content = finished["content"].as_array().unwrap();
    assert_eq!(content.len(), 2, "{content:?}");
    assert_eq!(content[0], terminal[0]);
    assert!(
        content[1]["content"]["text"]
            .as_str()
            .unwrap()
            .starts_with("Tool failed:"),
        "{content:?}"
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let tool_reply = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["tool_call_id"] == "call-ok")
        .unwrap()
        .clone();
    let output_text = format!("exit_code: 0\noutput:\n{CLIENT_TERMINAL_OUTPUT}");
    assert_eq!(tool_reply["content"], output_text.as_str());

    // A released terminal cannot be shown again, so replay shows the
    // command's output as text.
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", initialize).await;
    peer.request(
        "session/load",
        json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
    )
    .await;
    let replayed = updates_for(&peer.updates, "call-ok");
    let finished = replayed.last().unwrap();
    assert_eq!(finished["status"], "completed");
    assert_eq!(
        finished["content"],
        json!([{"type": "content", "content": {"type": "text", "text": output_text}}])
    );
    assert_eq!(finished["rawOutput"]["output"], CLIENT_TERMINAL_OUTPUT);
    let replayed = updates_for(&peer.updates, "call-broken");
    let finished = replayed.last().unwrap();
    assert_eq!(finished["status"], "failed");
    assert!(
        finished.to_string().contains("Tool failed:")
            && !finished.to_string().contains(CLIENT_TERMINAL_ID),
        "{finished}"
    );
    assert!(peer.terminal_requests.is_empty());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
}

#[tokio::test]
async fn a_session_whose_agent_died_during_a_tool_call_still_takes_prompts() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        encode_sse(&[
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-sleep","type":"function","function":{"name":"run_command","arguments":"{\"command\":\"sleep\",\"args\":[\"5\"]}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ]),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping interrupted tool test: loopback networking is unavailable");
        return;
    };
    let cwd = temp.path().to_string_lossy().to_string();
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": 999,
        "method": "session/prompt",
        "params": {"sessionId": session_id, "prompt": [{"type": "text", "text": "wait"}]}
    }))
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["toolCallId"] == "call-sleep"
                && message["params"]["update"]["status"] == "in_progress"
            {
                return;
            }
        }
    })
    .await
    .expect("the command should start");
    peer.child.kill().await.unwrap();

    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    peer.request(
        "session/load",
        json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
    )
    .await;
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["toolCallId"] == "call-sleep"
            && update["params"]["update"]["status"] == "failed"
    }));
    let response = peer
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "again"}]}),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let messages = requests[1]["messages"].as_array().unwrap();
    let call = messages
        .iter()
        .position(|message| !message["tool_calls"].is_null())
        .unwrap();
    assert_eq!(messages[call + 1]["role"], "tool");
    assert_eq!(messages[call + 1]["tool_call_id"], "call-sleep");
}

#[tokio::test]
async fn prompt_usage_counts_only_its_own_turn_and_says_so() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) =
        mock_sequence_server(vec![mock_text_stream(), mock_text_stream()]).await
    else {
        eprintln!("skipping turn usage test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    for text in ["first", "second"] {
        let response = peer
            .request(
                "session/prompt",
                json!({
                    "sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": text}]
                }),
            )
            .await;
        assert_eq!(response["usage"]["totalTokens"], 22, "{text}: {response}");
        assert_eq!(
            response["usage"]["_meta"]["mjolnir.dev/usage-scope"],
            "turn"
        );
    }

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn thinking_effort_is_advertised_and_sent_to_deepseek() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_single_text_server().await else {
        eprintln!("skipping effort test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let effort = session["configOptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|option| option["id"] == "reasoning_effort")
        .expect("session advertises a thinking effort option");
    assert_eq!(effort["category"], "thought_level");
    assert_eq!(effort["currentValue"], "high");
    assert_eq!(
        effort["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|option| option["value"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["none", "low", "high", "max"]
    );

    let set = peer
        .request(
            "session/set_config_option",
            json!({
                "sessionId": session["sessionId"],
                "configId": "reasoning_effort",
                "value": "max"
            }),
        )
        .await;
    let current = set["configOptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|option| option["id"] == "reasoning_effort")
        .unwrap();
    assert_eq!(current["currentValue"], "max");
    let rejected = peer
        .request_error(
            "session/set_config_option",
            json!({
                "sessionId": session["sessionId"],
                "configId": "reasoning_effort",
                "value": "turbo"
            }),
        )
        .await;
    assert_eq!(rejected["code"], -32602);

    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": "think hard"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let request = server.await.unwrap();
    assert_eq!(request["reasoning_effort"], "max");
    assert!(
        request.get("temperature").is_none(),
        "temperature must not be sent: {request}"
    );
}

#[tokio::test]
async fn model_request_failures_name_the_setting_to_check() {
    let Some(closed) = loopback_listener().await else {
        eprintln!("skipping request failure test: loopback networking is unavailable");
        return;
    };
    let refused = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let unauthorized = loopback_listener().await.unwrap();
    let rejecting = format!("http://{}", unauthorized.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = unauthorized.accept().await.unwrap();
        let _ = read_http_json_body(&mut socket).await;
        let body = r#"{"error":{"message":"Authentication Fails"}}"#;
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    for (base_url, expected) in [
        (refused, "DEEPSEEK_BASE_URL"),
        (rejecting, "DEEPSEEK_API_KEY"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        peer.request("initialize", json!({"protocolVersion": 1}))
            .await;
        let session = peer
            .request(
                "session/new",
                json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
            )
            .await;
        let error = peer
            .request_error(
                "session/prompt",
                json!({
                    "sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "hello"}]
                }),
            )
            .await;
        let message = error["data"]["message"].as_str().unwrap_or_default();
        assert!(message.contains(expected), "{error}");
        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    }
    server.await.unwrap();
}

#[tokio::test]
async fn allowing_a_command_for_the_session_allows_only_that_command() {
    let temp = tempfile::tempdir().unwrap();
    let command = |id: &str, text: &str| {
        encode_sse(&[
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"run_command","arguments":json!({"command":"sh","args":["-c",format!("printf {text}")]}).to_string()}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ])
    };
    let Some((base_url, server)) = mock_sequence_server(vec![
        command("call-a", "a"),
        command("call-a-again", "a"),
        command("call-b", "b"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping session approval test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.permission_answer = "allow_always";
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": "run commands"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    let titles: Vec<&str> = peer
        .permission_requests
        .iter()
        .filter_map(|request| request["params"]["toolCall"]["title"].as_str())
        .collect();
    assert_eq!(titles, ["Run sh -c 'printf a'", "Run sh -c 'printf b'"]);
    assert_eq!(
        peer.permission_requests[0]["params"]["options"][1]["name"],
        "Yes, and don't ask again for this command in this session"
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 4);
}

#[tokio::test]
async fn plan_mode_tells_the_model_and_refuses_writes_without_asking() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("sample.txt"), "sample file contents\n").unwrap();
    let Some((base_url, server)) = mock_deepseek_server().await else {
        eprintln!("skipping plan mode test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    peer.request(
        "session/set_mode",
        json!({"sessionId": session_id, "modeId": "plan"}),
    )
    .await;
    // The mock model asks for write_file anyway, as a model that ignores its
    // instructions would.
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Plan a change to sample.txt"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert!(peer.permission_requests.is_empty());
    assert_eq!(
        std::fs::read_to_string(temp.path().join("sample.txt")).unwrap(),
        "sample file contents\n"
    );
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["status"] == "failed"
            && update["params"]["update"]["rawOutput"]["reason"] == "plan_mode"
    }));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let offered: Vec<&str> = requests[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect();
    assert!(offered.contains(&"read_file"), "{offered:?}");
    for refused in ["write_file", "edit_file", "run_command"] {
        assert!(!offered.contains(&refused), "{offered:?}");
    }
    assert!(
        requests[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("The session is in plan mode.")
    );
}

#[tokio::test]
async fn plan_mode_allows_only_read_only_http_mcp_tools() {
    let temp = tempfile::tempdir().unwrap();
    let Some((mcp_url, mcp_server)) = mock_http_mcp_server().await else {
        eprintln!("skipping plan mode MCP test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) = mock_sequence_server(vec![
        mock_named_tool_call_stream("mcp__review__report"),
        mock_text_stream(),
        mock_named_tool_call_stream("mcp__review__mutate"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping plan mode MCP test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({
                    "cwd": temp.path().to_string_lossy(),
            "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}],
                    "additionalDirectories": []
                }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    peer.request(
        "session/set_mode",
        json!({"sessionId":session_id,"modeId":"plan"}),
    )
    .await;
    for input in ["report", "mutate"] {
        let response = peer
            .request(
                "session/prompt",
                json!({
                    "sessionId":session_id,"prompt":[{"type":"text","text":input}]
                }),
            )
            .await;
        assert_eq!(response["stopReason"], "end_turn");
        if input == "report" {
            assert_eq!(
                peer.permission_requests.len(),
                1,
                "read-only MCP still asks permission"
            );
        }
    }
    let requests = model_server.await.unwrap();
    let offered: Vec<&str> = requests[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect();
    assert!(offered.contains(&"mcp__review__report"), "{offered:?}");
    assert!(!offered.contains(&"mcp__review__mutate"), "{offered:?}");
    assert!(
        requests[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("read-only MCP tools")
    );
    assert_eq!(
        peer.permission_requests.len(),
        1,
        "mutate must be refused before permission"
    );
    assert!(
        peer.updates
            .iter()
            .any(|update| { update["params"]["update"]["rawOutput"]["reason"] == "plan_mode" })
    );
    let mcp_requests = mcp_server.await.unwrap();
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|request| request["method"] == "tools/call")
            .map(|request| request["params"]["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["report"],
        "MCP requests: {mcp_requests:?}"
    );
}

#[tokio::test]
async fn v2_effort_command_tells_the_client_the_new_setting() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer = spawn_peer(temp.path(), None).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();

    let accepted = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "/effort max"}]
            }),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("the effort command should settle to idle");

    // The client must hear about the change, or its selector keeps showing the
    // old value. A hand-written field list here once dropped this update.
    let announced = peer
        .updates
        .iter()
        .filter(|update| update["params"]["update"]["sessionUpdate"] == "config_option_update")
        .flat_map(|update| {
            update["params"]["update"]["configOptions"]
                .as_array()
                .cloned()
        })
        .flatten()
        .find(|option| option["configId"] == "reasoning_effort")
        .unwrap_or_else(|| panic!("no config update for effort: {:?}", peer.updates));
    assert_eq!(announced["currentValue"], "max");
}

#[tokio::test]
async fn v2_prompt_runs_local_tool_loop_and_permission_request() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("sample.txt"), "old contents").unwrap();
    let Some((base_url, server)) = mock_deepseek_server().await else {
        eprintln!("skipping v2 streaming test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let accepted = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Write sample.txt"}]
            }),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());

    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("v2 tool turn should settle to idle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    let usage_updates = peer
        .updates
        .iter()
        .filter(|update| update["params"]["update"]["sessionUpdate"] == "usage_update")
        .collect::<Vec<_>>();
    assert!(!usage_updates.is_empty());
    assert!(
        usage_updates
            .iter()
            .all(|update| update["params"]["update"]["used"] == 14
                || update["params"]["update"]["used"] == 22)
    );
    assert_eq!(
        usage_updates.last().unwrap()["params"]["update"]["used"],
        22
    );
    assert_eq!(peer.permission_requests.len(), 1);
    // The request shows the change before it is approved.
    let subject = &peer.permission_requests[0]["params"]["subject"];
    let preview = subject.to_string();
    assert!(
        preview.contains(r#""type":"diff""#)
            && preview.contains("-old contents")
            && preview.contains("+written by tool"),
        "{subject}"
    );
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["name"] == "write_file"
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["content"]
            .as_array()
            .is_some_and(|content| content.iter().any(|item| item["type"] == "diff"))
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "state_update"
            && update["params"]["update"]["state"] == "requires_action"
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && update["params"]["update"]["content"]["text"] == "mock answer"
    }));
    assert_eq!(
        std::fs::read_to_string(temp.path().join("sample.txt")).unwrap(),
        "written by tool"
    );

    let replay_start = peer.updates.len();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": [],
            "additionalDirectories": [],
            "replayFrom": {"type": "start"}
        }),
    )
    .await;
    let replayed = &peer.updates[replay_start..];
    assert!(replayed.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message"
            && update["params"]["update"]["content"][0]["text"] == "mock answer"
    }));
    assert!(replayed.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["status"] == "completed"
            && update["params"]["update"]["content"][0]["content"]["text"]
                .as_str()
                .is_some_and(|text| text.contains("Wrote"))
    }));
    assert!(replayed.iter().any(|update| {
        update["params"]["update"]["content"]
            .as_array()
            .is_some_and(|content| {
                content.iter().any(|item| {
                    item["type"] == "diff"
                        && item["patch"]["format"] == "git_patch"
                        && item["patch"]["text"].as_str().is_some_and(|patch| {
                            // Absolute on every platform: `/…` on Unix, `C:\…` on Windows.
                            patch.starts_with("diff --git ")
                                && patch
                                    .lines()
                                    .find_map(|line| line.strip_prefix("+++ "))
                                    .is_some_and(|path| {
                                        std::path::Path::new(path).is_absolute()
                                            && path.ends_with("sample.txt")
                                    })
                        })
                })
            })
    }));

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn v2_failed_tool_status_survives_replay() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_failed_tool_server().await else {
        eprintln!("skipping failed-tool replay test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "Trigger a failing tool"}]
        }),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            let update = &message["params"]["update"];
            if update["sessionUpdate"] == "state_update" && update["state"] == "idle" {
                break;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("failed tool turn should settle");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["status"] == "failed"
    }));

    let replay_start = peer.updates.len();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": [],
            "replayFrom": {"type": "start"}
        }),
    )
    .await;
    assert!(peer.updates[replay_start..].iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["status"] == "failed"
    }));

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn v2_steering_prompt_is_consumed_before_idle() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_steering_server().await else {
        eprintln!("skipping steering test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let first = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Start the long task"}]
            }),
        )
        .await;
    assert!(first["messageId"].as_str().is_some());
    let steering = peer
        .request(
            "_session/steering",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Steer toward the safer approach"}],
                "_meta": {"steering": {"idleBehavior": "promptRequired"}}
            }),
        )
        .await;
    assert_eq!(steering["outcome"], "injected");

    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            let update = &message["params"]["update"];
            if update["sessionUpdate"] == "state_update" && update["state"] == "idle" {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("steered turn should settle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && update["params"]["update"]["content"]["text"] == "steered answer"
    }));
    let idle_steer = peer
        .request(
            "_session/steering",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Keep this queued"}],
                "_meta": {"steering": {"idleBehavior": "promptRequired"}}
            }),
        )
        .await;
    assert_eq!(idle_steer["outcome"], "promptRequired");
    assert_eq!(idle_steer["reason"], "noRunningTurn");

    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["role"] == "user"
                    && message["content"]
                        .as_str()
                        .is_some_and(|content| content.contains("Steer toward the safer approach"))
            })
    );
}

#[tokio::test]
async fn session_cancel_ends_an_in_flight_stream() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping cancellation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {}, "terminal": false}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let prompt_id = peer.next_id;
    peer.next_id += 1;
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": prompt_id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "keep going until cancelled"}]
        }
    }))
    .await;

    let mut cancellation_sent = false;
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message.get("id").and_then(Value::as_u64) == Some(prompt_id) {
                return message;
            }
            if !cancellation_sent
                && message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                peer.write(json!({
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": {"sessionId": session_id}
                }))
                .await;
                cancellation_sent = true;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("cancellation should complete promptly");
    assert!(cancellation_sent);
    assert_eq!(response["result"]["stopReason"], "cancelled");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    server.abort();
}

#[tokio::test]
async fn v2_cancel_settles_foreground_work_to_idle_cancelled() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping v2 cancellation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "keep working"}]
        }),
    )
    .await;

    let mut resume_checked = false;
    let mut cancellation_sent = false;
    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if !cancellation_sent
                && message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                if !resume_checked {
                    let resumed = peer
                        .request(
                            "session/resume",
                            json!({
                                "sessionId": session_id,
                                "cwd": temp.path().to_string_lossy(),
                                "mcpServers": [],
                                "additionalDirectories": []
                            }),
                        )
                        .await;
                    assert!(resumed["configOptions"].is_array());
                    resume_checked = true;
                }
                peer.write(json!({
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": {"sessionId": session_id}
                }))
                .await;
                cancellation_sent = true;
            }
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("v2 cancellation should settle");
    assert!(resume_checked);
    assert!(cancellation_sent);
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    server.abort();
}

#[tokio::test]
async fn v2_command_reports_agent_owned_terminal_output() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_command_server().await else {
        eprintln!("skipping v2 terminal test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "run a command"}]
        }),
    )
    .await;
    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("terminal command should settle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "terminal_update"
            && update["params"]["update"]["command"]
                .as_str()
                .is_some_and(|command| command.starts_with("sh "))
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "terminal_output_chunk"
            && update["params"]["update"]["data"] == "dGVybWluYWwtb3V0cHV0"
    }));
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["content"]
            .as_array()
            .is_some_and(|content| content.iter().any(|item| item["type"] == "terminal"))
    }));
    let replay_start = peer.updates.len();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": [],
            "replayFrom": {"type": "start"}
        }),
    )
    .await;
    assert!(peer.updates[replay_start..].iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "terminal_update"
            && update["params"]["update"]["output"]["data"] == "dGVybWluYWwtb3V0cHV0"
    }));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn v2_command_runs_when_prompt_also_contains_an_image() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer = spawn_peer(temp.path(), None).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    )
    .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    peer.request(
        "session/set_config_option",
        json!({
            "sessionId": session_id,
            "configId": "model",
            "type": "id",
            "value": "deepseek-v4-pro"
        }),
    )
    .await;
    let accepted = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [
                    {"type": "text", "text": "/status"},
                    {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}
                ]
            }),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("local image command should settle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert!(peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && update["params"]["update"]["content"]["text"]
                .as_str()
                .is_some_and(|text| text.contains("model: deepseek-v4-pro"))
    }));
    drop(peer.stdin);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(status.success(), "server exited with {status}");
}

#[tokio::test]
async fn v2_image_prompt_uses_deepseek_image_content_parts() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_single_text_server().await else {
        eprintln!("skipping image test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    let initialize = peer
        .request(
            "initialize",
            json!({
                "protocolVersion": 2,
                "info": {"name": "v2-test-client", "version": "1.0.0"},
                "capabilities": {}
            }),
        )
        .await;
    assert_eq!(
        initialize["capabilities"]["session"]["prompt"]["image"],
        json!({})
    );
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [
                {"type": "text", "text": "What is shown?"},
                {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}
            ]
        }),
    )
    .await;
    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "state_update"
                && message["params"]["update"]["state"] == "idle"
            {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("image prompt should settle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    let replay_start = peer.updates.len();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": [],
            "replayFrom": {"type": "start"}
        }),
    )
    .await;
    assert!(peer.updates[replay_start..].iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "user_message"
            && update["params"]["update"]["content"]
                .as_array()
                .is_some_and(|content| {
                    content.iter().any(|block| {
                        block["type"] == "image"
                            && block["mimeType"] == "image/png"
                            && block["data"] == "aGVsbG8="
                    })
                })
    }));

    let request = server.await.unwrap();
    let user = request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap();
    assert!(user["content"].as_array().unwrap().iter().any(|part| {
        part["type"] == "image_url" && part["image_url"]["url"] == "data:image/png;base64,aGVsbG8="
    }));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
}

#[tokio::test]
async fn v1_image_prompt_uses_deepseek_image_content_parts() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_single_text_server().await else {
        eprintln!("skipping v1 image test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    let initialize = peer
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    assert_eq!(
        initialize["agentCapabilities"]["promptCapabilities"]["image"],
        true
    );
    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": temp.path().to_string_lossy(),
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [
                    {"type": "text", "text": "What is shown?"},
                    {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}
                ]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    let request = server.await.unwrap();
    let user = request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap();
    assert!(user["content"].as_array().unwrap().iter().any(|part| {
        part["type"] == "image_url" && part["image_url"]["url"] == "data:image/png;base64,aGVsbG8="
    }));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
}

async fn mock_deepseek_server() -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json_body(&mut socket).await;
            requests.push(body);
            let response = if index == 0 {
                mock_tool_call_stream()
            } else {
                mock_text_stream()
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn mock_slow_deepseek_server() -> Option<(String, tokio::task::JoinHandle<()>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_json_body(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let payload = encode_sse(&[json!({
            "choices": [{"delta": {"content": "working"}}]
        })]);
        socket
            .write_all(format!("{:x}\r\n{payload}\r\n", payload.len()).as_bytes())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });
    Some((format!("http://{address}"), task))
}

async fn mock_command_server() -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json_body(&mut socket).await;
            requests.push(body);
            let response = if index == 0 {
                encode_sse(&[
                    json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-command","type":"function","function":{"name":"run_command","arguments":"{\"command\":\"sh\",\"args\":[\"-c\",\"printf terminal-output\"]}"}}]}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
                    json!({"choices":[],"usage":{"prompt_tokens":8,"completion_tokens":3,"total_tokens":11}}),
                ])
            } else {
                mock_text_stream()
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

/// Serves `rounds` tool-call streams and then one text stream, so a test can
/// drive a prompt past any fixed cap on model/tool round trips.
async fn mock_long_tool_loop_server(
    rounds: usize,
) -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..=rounds {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json_body(&mut socket).await;
            requests.push(body);
            let response = if index < rounds {
                encode_sse(&[
                    json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":format!("call-{index}"),"type":"function","function":{"name":"run_command","arguments":"{\"command\":\"sh\",\"args\":[\"-c\",\"true\"]}"}}]}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
                    json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}),
                ])
            } else {
                mock_text_stream()
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn mock_failed_tool_server() -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json_body(&mut socket).await;
            requests.push(body);
            let response = if index == 0 {
                encode_sse(&[
                    json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-failed","type":"function","function":{"name":"write_file","arguments":"{\"path\":\"sample.txt\"}"}}]}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
                    json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}),
                ])
            } else {
                mock_text_stream()
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn mock_steering_server() -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json_body(&mut socket).await;
            requests.push(body);
            if index == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
            let response = if index == 0 {
                encode_sse(&[
                    json!({"choices":[{"delta":{"content":"initial answer"}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                    json!({"choices":[],"usage":{"prompt_tokens":8,"completion_tokens":2,"total_tokens":10}}),
                ])
            } else {
                encode_sse(&[
                    json!({"choices":[{"delta":{"content":"steered answer"}}]}),
                    json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                    json!({"choices":[],"usage":{"prompt_tokens":18,"completion_tokens":2,"total_tokens":20}}),
                ])
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn mock_single_text_server() -> Option<(String, tokio::task::JoinHandle<Value>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_http_json_body(&mut socket).await;
        let response = mock_text_stream();
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            response.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        request
    });
    Some((format!("http://{address}"), task))
}

/// A mock model that answers each request with the next response in order.
async fn mock_sequence_server(
    responses: Vec<String>,
) -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            requests.push(read_http_json_body(&mut socket).await);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn mock_http_mcp_server() -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_json_body(&mut socket).await;
            let response = match request["method"].as_str().unwrap() {
                "initialize" => json!({"jsonrpc":"2.0", "id":request["id"], "result":{
                    "protocolVersion":"2025-06-18", "capabilities":{"tools":{}},
                    "serverInfo":{"name":"mock", "version":"1"}
                }}),
                "notifications/initialized" => Value::Null,
                "tools/list" => json!({"jsonrpc":"2.0", "id":request["id"], "result":{"tools":[
                    {"name":"report", "description":"Report findings", "inputSchema":{"type":"object"}, "annotations":{"readOnlyHint":true}},
                    {"name":"mutate", "description":"Change state", "inputSchema":{"type":"object"}}
                ]}}),
                "tools/call" => {
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":{"content":[{"type":"text","text":"reported"}]}})
                }
                method => panic!("unexpected MCP method: {method}"),
            };
            requests.push(request.clone());
            if request.get("id").is_none() {
                socket
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            } else {
                let body = serde_json::to_vec(&response).unwrap();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: test-session\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

async fn loopback_listener() -> Option<TcpListener> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
        Err(error) => panic!("failed to bind loopback mock server: {error}"),
    }
}

async fn read_http_json_body(socket: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "HTTP request ended before headers");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "HTTP request ended before body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap()
}

fn mock_tool_call_stream() -> String {
    let events = [
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"write_file","arguments":"{\"path\":\"sample.txt\","}}]}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"content\":\"written by tool\"}"}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}),
    ];
    encode_sse(&events)
}

fn mock_named_tool_call_stream(name: &str) -> String {
    let events = [
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-mcp","type":"function","function":{"name":name,"arguments":"{}"}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}),
    ];
    encode_sse(&events)
}

fn mock_text_stream() -> String {
    let events = [
        json!({"choices":[{"delta":{"content":"mock answer"}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":2,"total_tokens":22}}),
    ];
    encode_sse(&events)
}

fn encode_sse(events: &[Value]) -> String {
    let mut output = String::new();
    for event in events {
        output.push_str("data: ");
        output.push_str(&serde_json::to_string(event).unwrap());
        output.push_str("\n\n");
    }
    output.push_str("data: [DONE]\n\n");
    output
}

// ---------------------------------------------------------------------------
// Failure modes that used to end a turn silently.
// ---------------------------------------------------------------------------

/// Answer the first request with `status` and a JSON error, then serve a text
/// stream. Returns how many requests were made.
async fn mock_fails_then_recovers(
    status: &'static str,
) -> Option<(String, tokio::task::JoinHandle<usize>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = 0;
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_json_body(&mut socket).await;
            requests += 1;
            let (head, body) = if index == 0 {
                let body = r#"{"error":{"message":"overloaded"}}"#.to_string();
                (
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    ),
                    body,
                )
            } else {
                let body = mock_text_stream();
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    ),
                    body,
                )
            };
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
            if index > 0 {
                break;
            }
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

/// Serve an empty 200 event-stream once, then a real answer.
async fn mock_empty_then_recovers() -> Option<(String, tokio::task::JoinHandle<usize>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = 0;
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_json_body(&mut socket).await;
            requests += 1;
            let body = if index == 0 {
                String::new()
            } else {
                mock_text_stream()
            };
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
            if index > 0 {
                break;
            }
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

/// Send a 200 event-stream whose body is `body`.
async fn mock_raw_stream(body: String) -> Option<(String, tokio::task::JoinHandle<usize>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = 0;
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_json_body(&mut socket).await;
            requests += 1;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

/// Stream four chunks 400ms apart, so the whole answer takes longer than the
/// configured timeout while no single gap reaches it.
async fn mock_slow_but_live_server() -> Option<(String, tokio::task::JoinHandle<()>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_json_body(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let chunks = [
            json!({"choices":[{"delta":{"content":"slow "}}]}),
            json!({"choices":[{"delta":{"content":"but "}}]}),
            json!({"choices":[{"delta":{"content":"alive"}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ];
        for (index, chunk) in chunks.iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
            let mut payload = format!("data: {chunk}\n\n");
            if index == chunks.len() - 1 {
                payload.push_str("data: [DONE]\n\n");
            }
            let framed = format!("{:x}\r\n{payload}\r\n", payload.len());
            socket.write_all(framed.as_bytes()).await.unwrap();
        }
        socket.write_all(b"0\r\n\r\n").await.unwrap();
        let _ = socket.shutdown().await;
    });
    Some((format!("http://{address}"), task))
}

async fn peer_raw_initialize(peer: &mut Peer) {
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {"fs": {}, "terminal": false}}),
    )
    .await;
}

async fn peer_prompt(peer: &mut Peer, cwd: &std::path::Path, text: &str) -> Value {
    let cwd = cwd.to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": text}]}),
    )
    .await
}

fn streamed_text(peer: &Peer) -> Vec<String> {
    peer.updates
        .iter()
        .filter(|update| update["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .map(|update| {
            update["params"]["update"]["content"]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn a_transient_api_failure_is_retried_instead_of_ending_the_turn() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_fails_then_recovers("503 Service Unavailable").await else {
        eprintln!("skipping retry test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let response = peer_prompt(&mut peer, temp.path(), "do the task").await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(streamed_text(&peer), ["mock answer"]);
    assert_eq!(
        server.await.unwrap(),
        2,
        "the failed request is retried once"
    );
}

#[tokio::test]
async fn a_rate_limited_request_is_retried() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_fails_then_recovers("429 Too Many Requests").await else {
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let response = peer_prompt(&mut peer, temp.path(), "do the task").await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(server.await.unwrap(), 2);
}

#[tokio::test]
async fn an_empty_response_is_retried_rather_than_reported_as_an_answer() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_empty_then_recovers().await else {
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let response = peer_prompt(&mut peer, temp.path(), "do the task").await;
    assert_eq!(
        response["stopReason"], "end_turn",
        "the retry should reach the real answer"
    );
    assert_eq!(streamed_text(&peer), ["mock answer"]);
    assert_eq!(server.await.unwrap(), 2, "the empty answer is retried once");
}

#[tokio::test]
async fn an_api_that_only_returns_empty_bodies_fails_loudly() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, _server)) = mock_raw_stream(String::new()).await else {
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let error = peer
        .request_error(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "do the task"}]}),
        )
        .await;
    assert_eq!(
        error["data"]["message"].as_str(),
        Some("The DeepSeek API returned an empty response."),
        "an empty answer is an error, never a finished turn"
    );
    assert!(streamed_text(&peer).is_empty());
}

#[tokio::test]
async fn a_truncated_response_is_an_error_not_a_finished_answer() {
    let temp = tempfile::tempdir().unwrap();
    let body = "data: {\"choices\":[{\"delta\":{\"content\":\"half an answer\"}}]}\n\n".to_string();
    let Some((base_url, _server)) = mock_raw_stream(body).await else {
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer_raw_initialize(&mut peer).await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let error = peer
        .request_error(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "do the task"}]}),
        )
        .await;
    let message = error["data"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("closed the connection before the response finished"),
        "unexpected error: {error}"
    );
    assert!(
        message.contains("partly streamed"),
        "the error must say why it was not retried: {error}"
    );
    assert_eq!(
        streamed_text(&peer),
        ["half an answer"],
        "partial text must not be duplicated by a retry"
    );
}

#[tokio::test]
async fn a_stream_that_keeps_producing_outlives_the_request_timeout() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, _server)) = mock_slow_but_live_server().await else {
        return;
    };
    // Every gap between chunks is 400ms; the whole answer takes ~1.2s.
    let mut peer =
        spawn_peer_with_args(temp.path(), Some(&base_url), &["--http-timeout-secs", "1"]).await;
    peer_raw_initialize(&mut peer).await;
    let response = peer_prompt(&mut peer, temp.path(), "write the file").await;
    assert_eq!(
        response["stopReason"], "end_turn",
        "a live stream must not be killed by the request timeout"
    );
    assert_eq!(streamed_text(&peer).concat(), "slow but alive");
}

/// Serve one response that ends at the token cap, carrying a half-written tool
/// call whose arguments were cut off mid-string.
async fn mock_truncated_with_tool_call() -> Option<(String, tokio::task::JoinHandle<usize>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let body = encode_sse(&[
        json!({"choices":[{"delta":{"content":"Writing the file"},"finish_reason":null}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-cut","type":"function","function":{"name":"write_file","arguments":"{\"file_path\":\"big.txt\",\"content\":\"half of a fi"}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"length"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":9,"total_tokens":19}}),
    ]);
    let task = tokio::spawn(async move {
        let mut requests = 0;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let _ = read_http_json_body(&mut socket).await;
            requests += 1;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if socket.write_all(head.as_bytes()).await.is_err() {
                break;
            }
            if socket.write_all(body.as_bytes()).await.is_err() {
                break;
            }
            let _ = socket.shutdown().await;
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

#[tokio::test]
async fn a_response_cut_off_at_the_token_limit_says_so_and_runs_no_tools() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_truncated_with_tool_call().await else {
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": {},
                "terminal": false,
                "session": {"notices": {}}
            }
        }),
    )
    .await;
    let response = peer_prompt(&mut peer, temp.path(), "write a big file").await;

    assert_eq!(response["stopReason"], "max_tokens");
    assert!(
        !temp.path().join("big.txt").exists(),
        "a half-written tool call must not run"
    );
    let notices: Vec<&Value> = peer
        .updates
        .iter()
        .map(|update| &update["params"]["update"])
        .filter(|update| update["sessionUpdate"] == "notice")
        .collect();
    assert_eq!(notices.len(), 1, "truncation must be reported, not silent");
    assert!(
        notices[0]["title"]
            .as_str()
            .unwrap_or_default()
            .contains("token limit"),
        "unexpected notice: {:?}",
        notices[0]
    );
    assert!(
        notices[0]["description"]
            .as_str()
            .unwrap_or_default()
            .contains("--max-tokens"),
        "the notice should say how to fix it: {:?}",
        notices[0]
    );
    let failed = peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "tool_call_update"
            && update["params"]["update"]["status"] == "failed"
    });
    assert!(failed, "the truncated tool call should be marked failed");
    server.abort();
}
