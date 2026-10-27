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
    /// Answer `session/request_permission` with this JSON-RPC error instead
    /// of `permission_answer`.
    permission_error: Option<Value>,
    /// Each `terminal/*` request the agent sent, with how many updates the
    /// peer had received by then.
    terminal_requests: Vec<(String, usize)>,
    /// Answer `terminal/wait_for_exit` with an error.
    fail_terminal_wait: bool,
    elicitation_requests: Vec<Value>,
    /// The action to answer `elicitation/create` with. `None` leaves it
    /// unanswered and returns it from `next_message`.
    elicitation_answer: Option<&'static str>,
    /// Each `elicitation/complete` the agent sent, with how many updates the
    /// peer had received by then.
    elicitation_completions: Vec<(Value, usize)>,
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

    /// The next message, without answering any request the agent sends.
    async fn read_message(&mut self) -> Value {
        let mut line = String::new();
        let read = self.stdout.read_line(&mut line).await.unwrap();
        assert!(read > 0, "server closed stdout before replying");
        serde_json::from_str(&line).expect("valid JSON-RPC")
    }

    async fn next_message(&mut self) -> Value {
        loop {
            let value = self.read_message().await;
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
                let reply = match &self.permission_error {
                    Some(error) => json!({"jsonrpc": "2.0", "id": value["id"], "error": error}),
                    None => json!({
                        "jsonrpc": "2.0",
                        "id": value["id"],
                        "result": {
                            "outcome": {
                                "outcome": "selected",
                                "optionId": self.permission_answer
                            }
                        }
                    }),
                };
                self.write(reply).await;
                continue;
            }
            if value.get("method").and_then(Value::as_str) == Some("elicitation/create") {
                self.elicitation_requests.push(value.clone());
                let Some(action) = self.elicitation_answer else {
                    return value;
                };
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": value["id"],
                    "result": {"action": action}
                }))
                .await;
                continue;
            }
            if value.get("method").and_then(Value::as_str) == Some("elicitation/complete") {
                self.elicitation_completions
                    .push((value, self.updates.len()));
                continue;
            }
            return value;
        }
    }

    async fn write(&mut self, value: Value) {
        self.write_together(&[value]).await;
    }

    /// Write messages in a single write, so the agent reads them together.
    async fn write_together(&mut self, values: &[Value]) {
        let mut bytes = Vec::new();
        for value in values {
            serde_json::to_writer(&mut bytes, value).unwrap();
            bytes.push(b'\n');
        }
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
    assert_eq!(idle["params"]["update"]["usage"]["totalTokens"], 0);
    assert_eq!(
        idle["params"]["update"]["usage"]["_meta"]["mjolnir.dev/usage-scope"],
        "turn"
    );

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

#[tokio::test]
async fn session_status_reports_sessions_without_loading_them() {
    let temp = tempfile::tempdir().unwrap();
    let cwd = temp.path().to_string_lossy().to_string();
    let mut peer = spawn_peer(temp.path(), None).await;
    let initialize = peer
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    assert_eq!(
        initialize["agentCapabilities"]["sessionCapabilities"]["status"],
        json!({})
    );
    let session_id = peer_new_session(&mut peer, &cwd).await;
    let closed_id = peer_new_session(&mut peer, &cwd).await;
    let deleted_id = peer_new_session(&mut peer, &cwd).await;
    peer.request("session/close", json!({"sessionId": closed_id}))
        .await;
    peer.request("session/delete", json!({"sessionId": deleted_id}))
        .await;

    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(status, json!({"status": "active"}));
    let status = peer
        .request("session/status", json!({"sessionId": closed_id}))
        .await;
    assert_eq!(status, json!({"status": "inactive"}));
    for unknown in [deleted_id.as_str(), "dsh-never-created", "../outside", ""] {
        let error = peer
            .request_error("session/status", json!({"sessionId": unknown}))
            .await;
        assert_eq!(
            error,
            json!({
                "code": -32002,
                "message": "Resource not found",
                "data": {"sessionId": unknown}
            }),
            "{unknown:?}"
        );
    }
    for params in [
        json!({}),
        json!({"sessionId": 7}),
        json!({"sessionId": null}),
    ] {
        let error = peer.request_error("session/status", params.clone()).await;
        assert_eq!(error["code"], -32602, "{params}");
    }
    let error = peer.request_error("session/unknown", json!({})).await;
    assert_eq!(error["code"], -32601);

    drop(peer.stdin);
    let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(exit.success(), "server exited with {exit}");

    // A new process has the sessions on disk but none in memory.
    let sessions = temp.path().join("sessions-data").join("sessions");
    let stored = || {
        let mut files = std::fs::read_dir(&sessions)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let modified = entry.metadata().unwrap().modified().unwrap();
                (
                    entry.file_name(),
                    modified,
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    };
    let before = stored();
    assert_eq!(before.len(), 2);
    let mut peer = spawn_peer(temp.path(), None).await;
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await;
    // Repeated, because a status that loaded the session would make the
    // second one report it active.
    for _ in 0..2 {
        let status = peer
            .request("session/status", json!({"sessionId": session_id}))
            .await;
        assert_eq!(status, json!({"status": "inactive"}));
    }
    let error = peer
        .request_error("session/status", json!({"sessionId": deleted_id}))
        .await;
    assert_eq!(error["code"], -32002);
    assert_eq!(stored(), before, "session/status changed the session store");

    peer.request(
        "session/resume",
        json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
    )
    .await;
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(status, json!({"status": "active"}));

    drop(peer.stdin);
    let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(exit.success(), "server exited with {exit}");
}

#[tokio::test]
async fn v2_session_status_reports_foreground_state() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("sample.txt"), "old contents").unwrap();
    let Some((base_url, server)) = mock_deepseek_server().await else {
        eprintln!("skipping v2 status test: loopback networking is unavailable");
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
    assert_eq!(initialize["capabilities"]["session"]["status"], json!({}));
    let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        status,
        json!({"status": "active", "state": {"state": "idle"}})
    );

    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "Write sample.txt"}]
        }),
    )
    .await;
    // Leave the permission request unanswered so the turn waits on the user.
    let permission = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let mut line = String::new();
            let read = peer.stdout.read_line(&mut line).await.unwrap();
            assert!(read > 0, "server closed stdout before asking permission");
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["method"] == "session/request_permission" {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("the write should ask permission");
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        status,
        json!({"status": "active", "state": {"state": "requires_action"}})
    );
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": permission["id"],
        "result": {"outcome": {"outcome": "selected", "optionId": "allow_once"}}
    }))
    .await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        status,
        json!({"status": "active", "state": {"state": "idle"}})
    );

    peer.request("session/close", json!({"sessionId": session_id}))
        .await;
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(status, json!({"status": "inactive"}));
    peer.request("session/delete", json!({"sessionId": session_id}))
        .await;
    let error = peer
        .request_error("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        error,
        json!({
            "code": -32002,
            "message": "Resource not found",
            "data": {"sessionId": session_id}
        })
    );
    let error = peer
        .request_error("session/status", json!({"sessionId": 7}))
        .await;
    assert_eq!(error["code"], -32602);
    let error = peer.request_error("session/unknown", json!({})).await;
    assert_eq!(error["code"], -32601);

    drop(peer.stdin);
    let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(exit.success(), "server exited with {exit}");
    assert_eq!(server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn v2_session_status_reports_a_running_turn() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping v2 running status test: loopback networking is unavailable");
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
    let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "keep working"}]
        }),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
                return;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("the turn should start streaming");
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        status,
        json!({"status": "active", "state": {"state": "running"}})
    );

    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");
    let status = peer
        .request("session/status", json!({"sessionId": session_id}))
        .await;
    assert_eq!(
        status,
        json!({"status": "active", "state": {"state": "idle"}})
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    server.abort();
}

#[tokio::test]
async fn malformed_session_status_params_are_answered_and_the_connection_keeps_serving() {
    let temp = tempfile::tempdir().unwrap();
    let initializes = [
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {}
        }),
    ];
    for initialize in initializes {
        let version = initialize["protocolVersion"].clone();
        // Stderr is piped and never read, like a client that ignores it. A
        // handler that returned its params error instead of answering it
        // logs a large warning, which fills the pipe and stalls the
        // connection (#53).
        let mut peer = spawn_peer_with_stderr(temp.path(), None, &[], Stdio::piped()).await;
        peer.request("initialize", initialize).await;
        let malformed = [
            None,
            Some(Value::Null),
            Some(json!([])),
            Some(json!({})),
            Some(json!({"sessionId": 7})),
            Some(json!({"sessionId": {"id": "dsh-x"}})),
        ];
        for params in malformed {
            let case = format!("v{version} params {params:?}");
            let id = peer.next_id;
            peer.next_id += 1;
            let mut request = json!({"jsonrpc": "2.0", "id": id, "method": "session/status"});
            if let Some(params) = params {
                request["params"] = params;
            }
            peer.write(request).await;
            let reply = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let message = peer.next_message().await;
                    if message["id"] == id {
                        return message;
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no reply to {case}"));
            assert_eq!(reply["error"]["code"], -32602, "{case}: {reply}");
        }
        // The connection still answers after every rejection.
        let list = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer.request("session/list", json!({})),
        )
        .await
        .unwrap_or_else(|_| panic!("v{version}: session/list went unanswered"));
        assert!(list["sessions"].is_array(), "v{version}: {list}");

        drop(peer.stdin);
        let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
            .await
            .expect("server exits on EOF")
            .unwrap();
        assert!(exit.success(), "v{version}: server exited with {exit}");
    }
}

/// The `initialize` params for ACP `version` 1 or 2.
fn initialize_params(version: u64) -> Value {
    if version == 1 {
        json!({"protocolVersion": 1, "clientCapabilities": {}})
    } else {
        json!({
            "protocolVersion": 2,
            "info": {"name": "rejection-test-client", "version": "1.0.0"},
            "capabilities": {}
        })
    }
}

/// Requests with invalid params, each answered with `-32602`: typed methods
/// the SDK rejects while parsing their params, and every `_session/steering`
/// check the agent makes itself. The session ID is never looked up.
fn rejected_requests(version: u64) -> Vec<(&'static str, Value)> {
    let session_methods: &[&str] = if version == 1 {
        &["session/new", "session/load", "session/resume"]
    } else {
        &["session/new", "session/resume"]
    };
    let mut requests: Vec<_> = session_methods
        .iter()
        .map(|method| (*method, json!({"cwd": 5})))
        .collect();
    let text = json!([{"type": "text", "text": "steer"}]);
    for params in [
        json!({}),
        json!({"sessionId": ""}),
        json!({"sessionId": "dsh-x"}),
        json!({"sessionId": "dsh-x", "prompt": "steer"}),
        json!({"sessionId": "dsh-x", "prompt": []}),
        json!({
            "sessionId": "dsh-x",
            "prompt": text,
            "_meta": {"steering": {"idleBehavior": "queue"}}
        }),
    ] {
        requests.push(("_session/steering", params));
    }
    requests
}

#[tokio::test]
async fn rejected_requests_log_little_and_the_connection_keeps_serving() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let stderr_path = temp.path().join("stderr.log");
        let stderr = std::fs::File::create(&stderr_path).unwrap();
        let mut peer = spawn_peer_with_stderr(temp.path(), None, &[], stderr.into()).await;
        peer.request("initialize", initialize_params(version)).await;
        let requests = rejected_requests(version);
        for (method, params) in &requests {
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                peer.request_error(method, params.clone()),
            )
            .await
            .unwrap_or_else(|_| panic!("v{version}: no reply to {method} {params}"));
            assert_eq!(
                error["code"], -32602,
                "v{version}: {method} {params}: {error}"
            );
        }
        let list = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer.request("session/list", json!({})),
        )
        .await
        .unwrap_or_else(|_| panic!("v{version}: session/list went unanswered"));
        assert!(list["sessions"].is_array(), "v{version}: {list}");

        drop(peer.stdin);
        let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
            .await
            .expect("server exits on EOF")
            .unwrap();
        assert!(exit.success(), "v{version}: server exited with {exit}");

        // Each rejection used to log about 1 MB: the SDK's warning for a
        // handler that returned an error describes the whole handler chain,
        // escaped again at every link (#53). The SDK's warning for the error
        // response it sends stays, so the rejections are still logged.
        let logged = std::fs::read_to_string(&stderr_path).unwrap();
        assert!(
            logged.len() < 16 * 1024,
            "v{version}: {} rejections logged {} bytes",
            requests.len(),
            logged.len()
        );
        for (method, _) in &requests {
            assert!(logged.contains(method), "v{version}: {method}: {logged}");
        }
    }
}

#[tokio::test]
async fn a_burst_of_rejected_requests_does_not_fill_an_unread_stderr_pipe() {
    // Each rejection still logs one warning of a few hundred bytes. Linux
    // pipes hold 64 KiB, but macOS and Windows pipes can hold much less, so
    // the burst is smaller there. Before #53 a single rejection logged about
    // 1 MB, which filled any pipe and stalled the connection.
    let burst = if cfg!(target_os = "linux") { 50 } else { 6 };
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        // Piped and never read, like a client that ignores stderr.
        let mut peer = spawn_peer_with_stderr(temp.path(), None, &[], Stdio::piped()).await;
        peer.request("initialize", initialize_params(version)).await;
        let first_id = peer.next_id;
        let requests: Vec<Value> = rejected_requests(version)
            .into_iter()
            .cycle()
            .take(burst)
            .zip(first_id..)
            .map(|((method, params), id)| {
                json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
            })
            .collect();
        peer.next_id += burst as u64;
        peer.write_together(&requests).await;
        let mut unanswered: std::collections::BTreeSet<u64> = (first_id..peer.next_id).collect();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !unanswered.is_empty() {
                let reply = peer.next_message().await;
                let id = reply["id"].as_u64().expect("a response");
                assert_eq!(reply["error"]["code"], -32602, "v{version}: {reply}");
                assert!(unanswered.remove(&id), "v{version}: {reply}");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("v{version}: unanswered: {unanswered:?}"));
        let list = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer.request("session/list", json!({})),
        )
        .await
        .unwrap_or_else(|_| panic!("v{version}: session/list went unanswered"));
        assert!(list["sessions"].is_array(), "v{version}: {list}");

        drop(peer.stdin);
        let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
            .await
            .expect("server exits on EOF")
            .unwrap();
        assert!(exit.success(), "v{version}: server exited with {exit}");
    }
}

#[tokio::test]
async fn v2_session_status_answers_while_an_mcp_call_is_in_flight() {
    let temp = tempfile::tempdir().unwrap();
    let (started, call_started) = tokio::sync::oneshot::channel();
    let (release_call, release) = tokio::sync::oneshot::channel();
    // initialize, notifications/initialized, and tools/list when the server
    // connects, a tools/list refresh before each model request, and the call.
    let held = HeldMcpCall { started, release };
    let Some((mcp_url, mcp_server)) = mock_http_mcp_server(6, Some(held)).await else {
        eprintln!("skipping v2 MCP status test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) = mock_sequence_server(vec![
        mock_named_tool_call_stream("mcp__review__report"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping v2 MCP status test: loopback networking is unavailable");
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
            json!({
                "cwd": cwd,
                "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}]
            }),
        )
        .await;
    let busy_id = session["sessionId"].as_str().unwrap().to_string();
    let idle_id = peer_new_session(&mut peer, &cwd).await;
    peer.request(
        "session/prompt",
        json!({"sessionId": busy_id, "prompt": [{"type": "text", "text": "report"}]}),
    )
    .await;
    // Reading answers the permission request on the way.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            let update = &message["params"]["update"];
            if update["sessionUpdate"] == "tool_call_update" && update["status"] == "in_progress" {
                return;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("the MCP tool call should start");
    tokio::time::timeout(std::time::Duration::from_secs(5), call_started)
        .await
        .expect("the MCP call should reach the server")
        .unwrap();

    // The mock holds the call's reply, so the turn keeps the session state
    // lock until `release_call`. Status must not wait for it, for this
    // session or another one on the connection.
    for (session_id, state) in [(&busy_id, "running"), (&idle_id, "idle")] {
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            peer.request("session/status", json!({"sessionId": session_id})),
        )
        .await
        .expect("session/status waited for the MCP call");
        assert_eq!(
            status,
            json!({"status": "active", "state": {"state": state}}),
            "{state}"
        );
    }
    release_call.send(()).unwrap();
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");

    drop(peer.stdin);
    let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(exit.success(), "server exited with {exit}");
    let mcp_requests = served_mcp_requests(mcp_server).await;
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|request| request["method"] == "tools/call")
            .count(),
        1,
        "MCP requests: {mcp_requests:?}"
    );
    assert_eq!(model_server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_rejected_initialize_is_answered_before_the_server_exits() {
    let temp = tempfile::tempdir().unwrap();
    let cases = [
        (Some(json!(0)), -32022),
        (None, -32602),
        (Some(json!("2")), -32602),
        (Some(Value::Null), -32602),
        (Some(json!(-1)), -32602),
        (Some(json!(65536)), -32602),
    ];
    for (protocol_version, code) in cases {
        let case = format!("protocolVersion {protocol_version:?}");
        let mut peer = spawn_peer(temp.path(), None).await;
        let mut params = json!({"info": {"name": "tck", "version": "0"}, "capabilities": {}});
        if let Some(protocol_version) = &protocol_version {
            params["protocolVersion"] = protocol_version.clone();
        }
        // Stdin stays open, which is where the response was most often lost.
        peer.write(json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": params}))
            .await;
        let mut line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer.stdout.read_line(&mut line),
        )
        .await
        .unwrap_or_else(|_| panic!("no reply to {case}"))
        .unwrap();
        assert!(!line.is_empty(), "server exited without answering {case}");
        let reply: Value = serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("{case}: invalid JSON-RPC: {error}"));
        assert_eq!(reply["id"], 1, "{case}");
        let error = &reply["error"];
        assert_eq!(error["code"], code, "{case}");
        if code == -32022 {
            assert_eq!(
                error["data"],
                json!({"requested": 0, "supported": [1, 2]}),
                "{case}"
            );
        }
        let status = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
            .await
            .unwrap_or_else(|_| panic!("server did not exit after rejecting {case}"))
            .unwrap();
        assert!(status.success(), "{case}: server exited with {status}");
    }
}

/// The protocol version under test and its names for the auth methods.
struct AuthProtocol {
    version: u64,
    login: &'static str,
    logout: &'static str,
}

const AUTH_PROTOCOLS: [AuthProtocol; 2] = [
    AuthProtocol {
        version: 1,
        login: "authenticate",
        logout: "logout",
    },
    AuthProtocol {
        version: 2,
        login: "auth/login",
        logout: "auth/logout",
    },
];

/// Initializes `peer` and checks that its auth capabilities are exactly the
/// existing ones plus the `auth/status` marker for this protocol version.
async fn initialize_advertising_auth_status(peer: &mut Peer, protocol: &AuthProtocol) {
    let v = protocol.version;
    let (auth, expected) = if v == 1 {
        let result = peer
            .request(
                "initialize",
                json!({"protocolVersion": 1, "clientCapabilities": {}}),
            )
            .await;
        // The RFD's v1 example uses a boolean.
        (
            result["agentCapabilities"]["auth"].clone(),
            json!({"logout": {}, "status": true}),
        )
    } else {
        let result = peer
            .request(
                "initialize",
                json!({
                    "protocolVersion": 2,
                    "info": {"name": "auth-status-test", "version": "1.0.0"},
                    "capabilities": {}
                }),
            )
            .await;
        // v2 capability markers are objects, and v2 has no logout marker.
        (
            result["capabilities"]["auth"].clone(),
            json!({"status": {}}),
        )
    };
    assert_eq!(auth, expected, "v{v}: auth capabilities");
}

/// Asks for the auth state and checks that the answer leaks no credential.
async fn auth_status(peer: &mut Peer, secrets: &[&str]) -> (bool, String) {
    let status = peer.request("auth/status", json!({})).await;
    let text = status.to_string();
    for secret in secrets {
        assert!(
            !text.contains(secret),
            "auth/status leaked a credential: {text}"
        );
    }
    (
        status["authenticated"]
            .as_bool()
            .expect("authenticated is a boolean"),
        status["message"]
            .as_str()
            .expect("message is a string")
            .to_string(),
    )
}

#[tokio::test]
async fn auth_status_reports_the_configured_key_until_sign_out() {
    let secrets = ["test-key", "header-secret"];
    for protocol in &AUTH_PROTOCOLS {
        let v = protocol.version;
        let temp = tempfile::tempdir().unwrap();
        // The base URL is never contacted: the state is local.
        let mut peer = spawn_peer(temp.path(), Some("http://127.0.0.1:9")).await;
        initialize_advertising_auth_status(&mut peer, protocol).await;

        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(authenticated, "v{v}: a configured key: {message}");
        assert!(message.contains("DEEPSEEK_API_KEY"), "v{v}: {message}");
        assert_eq!(
            auth_status(&mut peer, &secrets).await,
            (authenticated, message),
            "v{v}: asking again changes nothing"
        );

        peer.request(protocol.logout, json!({})).await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(!authenticated, "v{v}: after {}: {message}", protocol.logout);
        assert!(message.contains("disabled"), "v{v}: {message}");

        peer.request(protocol.login, json!({"methodId": "deepseek-api-key"}))
            .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(authenticated, "v{v}: after {}: {message}", protocol.login);

        peer.request("providers/disable", json!({"providerId": "deepseek"}))
            .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(!authenticated, "v{v}: after providers/disable: {message}");
        assert!(message.contains("disabled"), "v{v}: {message}");

        peer.request(
            "providers/set",
            json!({
                "providerId": "deepseek",
                "apiType": "openai",
                "baseUrl": "http://127.0.0.1:9",
                "headers": {"Authorization": "Bearer header-secret"}
            }),
        )
        .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(authenticated, "v{v}: after providers/set: {message}");
        assert!(message.contains("Authorization header"), "v{v}: {message}");

        // `_meta` is the only request field, and it changes nothing.
        let status = peer
            .request("auth/status", json!({"_meta": {"trace": "abc"}}))
            .await;
        assert_eq!(status["authenticated"], true, "v{v}: with _meta");
    }
}

#[tokio::test]
async fn auth_status_reports_missing_credentials() {
    let secrets = ["header-secret"];
    for protocol in &AUTH_PROTOCOLS {
        let v = protocol.version;
        let temp = tempfile::tempdir().unwrap();
        let mut peer = spawn_peer(temp.path(), None).await;
        initialize_advertising_auth_status(&mut peer, protocol).await;

        // Params are optional.
        peer.write(json!({"jsonrpc": "2.0", "id": 900, "method": "auth/status"}))
            .await;
        let reply = peer.next_message().await;
        assert_eq!(reply["id"], 900, "v{v}: {reply}");
        assert_eq!(reply["result"]["authenticated"], false, "v{v}: {reply}");

        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(!authenticated, "v{v}: no key: {message}");
        assert!(
            message.contains("No DeepSeek credentials"),
            "v{v}: {message}"
        );

        let login = peer
            .request_error(protocol.login, json!({"methodId": "deepseek-api-key"}))
            .await;
        assert_eq!(login["code"], -32000, "v{v}: {login}");
        assert_eq!(
            auth_status(&mut peer, &secrets).await,
            (authenticated, message),
            "v{v}: a failed {} changes nothing",
            protocol.login
        );

        peer.request(
            "providers/set",
            json!({
                "providerId": "deepseek",
                "apiType": "openai",
                "baseUrl": "http://127.0.0.1:9",
                "headers": {"X-Trace": "1"}
            }),
        )
        .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(
            !authenticated,
            "v{v}: headers without Authorization: {message}"
        );
        assert!(
            message.contains("No DeepSeek credentials"),
            "v{v}: {message}"
        );

        peer.request(
            "providers/set",
            json!({
                "providerId": "deepseek",
                "apiType": "openai",
                "baseUrl": "http://127.0.0.1:9",
                "headers": {"authorization": "Bearer header-secret"}
            }),
        )
        .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(
            authenticated,
            "v{v}: provider Authorization header: {message}"
        );
        assert!(message.contains("Authorization header"), "v{v}: {message}");

        peer.request(protocol.logout, json!({})).await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(!authenticated, "v{v}: after {}: {message}", protocol.logout);
        assert!(message.contains("disabled"), "v{v}: {message}");

        peer.request(protocol.login, json!({"methodId": "deepseek-api-key"}))
            .await;
        let (authenticated, message) = auth_status(&mut peer, &secrets).await;
        assert!(authenticated, "v{v}: after {}: {message}", protocol.login);

        let unknown = peer.request_error("auth/unknown", json!({})).await;
        assert_eq!(unknown["code"], -32601, "v{v}: {unknown}");
    }
}

/// Initializes `peer` with ACP `version` and checks that it advertises
/// `session/set_title` with an empty marker.
async fn initialize_advertising_set_title(peer: &mut Peer, version: u64) {
    let params = if version == 1 {
        json!({"protocolVersion": 1, "clientCapabilities": {}})
    } else {
        json!({
            "protocolVersion": 2,
            "info": {"name": "set-title-test", "version": "1.0.0"},
            "capabilities": {}
        })
    };
    let result = peer.request("initialize", params).await;
    let marker = if version == 1 {
        &result["agentCapabilities"]["sessionCapabilities"]["setTitle"]
    } else {
        &result["capabilities"]["session"]["setTitle"]
    };
    assert_eq!(marker, &json!({}), "v{version}: {result}");
}

/// The title `session/list` reports for `session_id`.
async fn listed_title(peer: &mut Peer, session_id: &str) -> Value {
    let list = peer.request("session/list", json!({})).await;
    list["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|session| session["sessionId"] == session_id)
        .map(|session| session["title"].clone())
        .unwrap_or_else(|| panic!("{session_id} is not listed: {list}"))
}

/// The titles in the `session_info_update`s for `session_id` from
/// `peer.updates[from..]`, in order.
fn announced_titles(peer: &Peer, from: usize, session_id: &str) -> Vec<Value> {
    peer.updates[from..]
        .iter()
        .filter(|message| {
            message["params"]["sessionId"] == session_id
                && message["params"]["update"]["sessionUpdate"] == "session_info_update"
        })
        .map(|message| message["params"]["update"]["title"].clone())
        .collect()
}

/// Sends a prompt and waits for its turn to end.
async fn prompt_until_end_turn(peer: &mut Peer, version: u64, session_id: &str, text: &str) {
    let result = peer
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": text}]}),
        )
        .await;
    if version == 1 {
        assert_eq!(result["stopReason"], "end_turn", "{result}");
    } else {
        let idle = next_v2_idle(peer).await;
        assert_eq!(idle["params"]["update"]["stopReason"], "end_turn", "{idle}");
    }
}

async fn exit_peer(mut peer: Peer) {
    drop(peer.stdin);
    let exit = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait())
        .await
        .expect("server exits on EOF")
        .unwrap();
    assert!(exit.success(), "server exited with {exit}");
}

fn stored_record(data_dir: &std::path::Path, session_id: &str) -> Value {
    let path = data_dir
        .join("sessions-data")
        .join("sessions")
        .join(format!("{session_id}.json"));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn set_title_renames_a_loaded_session_and_later_prompts_keep_it() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream()]).await else {
            eprintln!("skipping set_title test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;

        let from = peer.updates.len();
        let result = peer
            .request(
                "session/set_title",
                json!({"sessionId": session_id, "title": "  Investigate\n\tOAuth   timeout  "}),
            )
            .await;
        assert_eq!(
            result,
            json!({"title": "Investigate OAuth timeout"}),
            "v{version}"
        );
        assert_eq!(
            announced_titles(&peer, from, &session_id),
            [json!("Investigate OAuth timeout")],
            "v{version}: the loaded session is told"
        );
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Investigate OAuth timeout",
            "v{version}"
        );

        // The first prompt would otherwise title the session after itself.
        let from = peer.updates.len();
        prompt_until_end_turn(&mut peer, version, &session_id, "Write the release notes").await;
        let announced = announced_titles(&peer, from, &session_id);
        assert!(
            !announced.is_empty(),
            "v{version}: the turn sends its title"
        );
        assert!(
            announced
                .iter()
                .all(|title| title == "Investigate OAuth timeout"),
            "v{version}: {announced:?}"
        );
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Investigate OAuth timeout",
            "v{version}"
        );
        let record = stored_record(temp.path(), &session_id);
        assert_eq!(record["title"], "Investigate OAuth timeout", "v{version}");
        assert_eq!(record["title_set_by_client"], true, "v{version}");

        // A later rename replaces the client's own title, capped in characters.
        let result = peer
            .request(
                "session/set_title",
                json!({"sessionId": session_id, "title": "ü".repeat(300)}),
            )
            .await;
        assert_eq!(result, json!({"title": "ü".repeat(256)}), "v{version}");
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "ü".repeat(256),
            "v{version}"
        );

        exit_peer(peer).await;
        assert_eq!(server.await.unwrap().len(), 1, "v{version}");
    }
}

#[tokio::test]
async fn set_title_renames_a_session_that_is_not_loaded() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream()]).await else {
            eprintln!("skipping set_title test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
        prompt_until_end_turn(&mut peer, version, &session_id, "Draft the migration plan").await;
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Draft the migration plan",
            "v{version}: a generated title"
        );
        exit_peer(peer).await;
        server.await.unwrap();
        let mut expected = stored_record(temp.path(), &session_id);
        assert_eq!(expected["title_set_by_client"], false, "v{version}");

        // A new process has the session on disk only.
        let mut peer = spawn_peer(temp.path(), None).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let result = peer
            .request(
                "session/set_title",
                json!({"sessionId": session_id, "title": "Migration plan"}),
            )
            .await;
        assert_eq!(result, json!({"title": "Migration plan"}), "v{version}");
        assert!(
            peer.updates.is_empty(),
            "v{version}: no update for a session this connection has not loaded: {:?}",
            peer.updates
        );
        let status = peer
            .request("session/status", json!({"sessionId": session_id}))
            .await;
        assert_eq!(status["status"], "inactive", "v{version}: not loaded");
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Migration plan",
            "v{version}"
        );
        expected["title"] = json!("Migration plan");
        expected["title_set_by_client"] = json!(true);
        assert_eq!(
            stored_record(temp.path(), &session_id),
            expected,
            "v{version}: only the title changed"
        );
        exit_peer(peer).await;

        let mut peer = spawn_peer(temp.path(), None).await;
        initialize_advertising_set_title(&mut peer, version).await;
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Migration plan",
            "v{version}: after a restart"
        );
        exit_peer(peer).await;
    }
}

#[tokio::test]
async fn set_title_rejects_empty_titles_unknown_sessions_and_malformed_params() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        // Stderr is piped and never read, so a handler that returned its
        // params error instead of answering it would stall the connection
        // (#53).
        let mut peer = spawn_peer_with_stderr(temp.path(), None, &[], Stdio::piped()).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let cwd = temp.path().to_string_lossy().to_string();
        let session_id = peer_new_session(&mut peer, &cwd).await;
        let deleted_id = peer_new_session(&mut peer, &cwd).await;
        peer.request("session/delete", json!({"sessionId": deleted_id}))
            .await;
        peer.request(
            "session/set_title",
            json!({"sessionId": session_id, "title": "Kept"}),
        )
        .await;

        for title in ["", "   ", "\n\t\r\n", "\u{7}"] {
            let error = peer
                .request_error(
                    "session/set_title",
                    json!({"sessionId": session_id, "title": title}),
                )
                .await;
            assert_eq!(
                error,
                json!({"code": -32602, "message": "Title must not be empty."}),
                "v{version}: {title:?}"
            );
        }
        for unknown in [deleted_id.as_str(), "dsh-never-created", "../outside", ""] {
            let error = peer
                .request_error(
                    "session/set_title",
                    json!({"sessionId": unknown, "title": "Lost"}),
                )
                .await;
            assert_eq!(
                error,
                json!({
                    "code": -32002,
                    "message": "Resource not found",
                    "data": {"sessionId": unknown}
                }),
                "v{version}: {unknown:?}"
            );
        }

        let malformed = [
            None,
            Some(Value::Null),
            Some(json!([])),
            Some(json!({})),
            Some(json!({"title": "No session"})),
            Some(json!({"sessionId": 7, "title": "Number"})),
            Some(json!({"sessionId": session_id})),
            Some(json!({"sessionId": session_id, "title": null})),
            Some(json!({"sessionId": session_id, "title": 7})),
            Some(json!({"sessionId": session_id, "title": ["Kept"]})),
        ];
        for params in malformed {
            let case = format!("v{version} params {params:?}");
            let id = peer.next_id;
            peer.next_id += 1;
            let mut request = json!({"jsonrpc": "2.0", "id": id, "method": "session/set_title"});
            if let Some(params) = params {
                request["params"] = params;
            }
            peer.write(request).await;
            let reply = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let message = peer.next_message().await;
                    if message["id"] == id {
                        return message;
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no reply to {case}"));
            assert_eq!(reply["error"]["code"], -32602, "{case}: {reply}");
        }

        // The connection still answers, and nothing was renamed or recreated.
        let list = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            peer.request("session/list", json!({})),
        )
        .await
        .unwrap_or_else(|_| panic!("v{version}: session/list went unanswered"));
        let sessions = list["sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 1, "v{version}: {list}");
        assert_eq!(sessions[0]["sessionId"], session_id, "v{version}");
        assert_eq!(sessions[0]["title"], "Kept", "v{version}");
        exit_peer(peer).await;
    }
}

#[tokio::test]
async fn set_title_during_a_turn_keeps_the_new_title() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, release, server)) = mock_gated_deepseek_server().await else {
            eprintln!("skipping set_title test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
        let prompt = json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "Investigate the flaky test"}]
        });
        // A v1 prompt is answered when its turn ends, a v2 one when accepted.
        let prompt_id = peer.next_id;
        if version == 1 {
            peer.next_id += 1;
            peer.write(json!({
                "jsonrpc": "2.0",
                "id": prompt_id,
                "method": "session/prompt",
                "params": prompt
            }))
            .await;
        } else {
            peer.request("session/prompt", prompt).await;
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !peer
                .updates
                .iter()
                .any(|update| update["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            {
                let message = peer.next_message().await;
                peer.updates.push(message);
            }
        })
        .await
        .expect("the model stream should start");
        assert_eq!(
            announced_titles(&peer, 0, &session_id),
            [json!("Investigate the flaky test")],
            "v{version}: the turn titled the session"
        );

        // The gated model cannot finish the turn until `release`.
        let from = peer.updates.len();
        let result = peer
            .request(
                "session/set_title",
                json!({"sessionId": session_id, "title": "Flaky test hunt"}),
            )
            .await;
        assert_eq!(result, json!({"title": "Flaky test hunt"}), "v{version}");
        release.send(()).unwrap();
        if version == 1 {
            let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let message = peer.next_message().await;
                    if message["id"] == prompt_id {
                        return message;
                    }
                    peer.updates.push(message);
                }
            })
            .await
            .expect("the turn should end");
            assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");
        } else {
            let idle = next_v2_idle(&mut peer).await;
            assert_eq!(idle["params"]["update"]["stopReason"], "end_turn", "{idle}");
        }
        assert_eq!(
            announced_titles(&peer, from, &session_id),
            [json!("Flaky test hunt")],
            "v{version}"
        );
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Flaky test hunt",
            "v{version}: the turn's saves keep the new title"
        );
        exit_peer(peer).await;
        server.await.unwrap();
    }
}

#[tokio::test]
async fn sessions_stored_without_the_title_flag_still_get_generated_titles() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().to_string_lossy().to_string();
        let mut peer = spawn_peer(temp.path(), None).await;
        initialize_advertising_set_title(&mut peer, version).await;
        let session_id = peer_new_session(&mut peer, &cwd).await;
        exit_peer(peer).await;

        // Make the record look like one stored before `session/set_title`.
        let path = temp
            .path()
            .join("sessions-data")
            .join("sessions")
            .join(format!("{session_id}.json"));
        let mut record = stored_record(temp.path(), &session_id);
        assert_eq!(record["title"], Value::Null, "v{version}");
        assert!(
            record
                .as_object_mut()
                .unwrap()
                .remove("title_set_by_client")
                .is_some(),
            "v{version}"
        );
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();

        let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream()]).await else {
            eprintln!("skipping set_title test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        initialize_advertising_set_title(&mut peer, version).await;
        peer.request(
            "session/resume",
            json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
        )
        .await;
        let from = peer.updates.len();
        prompt_until_end_turn(&mut peer, version, &session_id, "Summarize the open issues").await;
        assert!(
            announced_titles(&peer, from, &session_id)
                .iter()
                .any(|title| title == "Summarize the open issues"),
            "v{version}: {:?}",
            peer.updates
        );
        assert_eq!(
            listed_title(&mut peer, &session_id).await,
            "Summarize the open issues",
            "v{version}"
        );
        assert_eq!(
            stored_record(temp.path(), &session_id)["title_set_by_client"],
            false,
            "v{version}"
        );
        exit_peer(peer).await;
        server.await.unwrap();
    }
}

async fn spawn_peer(data_dir: &std::path::Path, base_url: Option<&str>) -> Peer {
    spawn_peer_with_args(data_dir, base_url, &[]).await
}

async fn spawn_peer_with_args(
    data_dir: &std::path::Path,
    base_url: Option<&str>,
    extra_args: &[&str],
) -> Peer {
    let api_key = base_url.map(|_| "test-key");
    spawn_peer_with_api_key(data_dir, base_url, api_key, extra_args).await
}

/// Like `spawn_peer_with_args`, but `DEEPSEEK_API_KEY` is set to `api_key`,
/// or removed when it is `None`, whatever the base URL.
async fn spawn_peer_with_api_key(
    data_dir: &std::path::Path,
    base_url: Option<&str>,
    api_key: Option<&str>,
    extra_args: &[&str],
) -> Peer {
    spawn_peer_with_options(data_dir, base_url, api_key, extra_args, Stdio::null()).await
}

/// Like [`spawn_peer_with_args`], with the server's stderr sent to `stderr`.
async fn spawn_peer_with_stderr(
    data_dir: &std::path::Path,
    base_url: Option<&str>,
    extra_args: &[&str],
    stderr: Stdio,
) -> Peer {
    let api_key = base_url.map(|_| "test-key");
    spawn_peer_with_options(data_dir, base_url, api_key, extra_args, stderr).await
}

async fn spawn_peer_with_options(
    data_dir: &std::path::Path,
    base_url: Option<&str>,
    api_key: Option<&str>,
    extra_args: &[&str],
    stderr: Stdio,
) -> Peer {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dsh-acp"));
    command
        .arg("--data-dir")
        .arg(data_dir.join("sessions-data"))
        .arg("--disable-web")
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr)
        // The default log filter, whatever the test's environment: tests that
        // pipe stderr depend on how much the server logs.
        .env_remove("RUST_LOG");
    if let Some(base_url) = base_url {
        command
            .arg("--base-url")
            .arg(base_url)
            .arg("--permission-mode")
            .arg("ask");
    }
    match api_key {
        Some(api_key) => command.env("DEEPSEEK_API_KEY", api_key),
        None => command.env_remove("DEEPSEEK_API_KEY"),
    };
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
        permission_error: None,
        terminal_requests: Vec::new(),
        fail_terminal_wait: false,
        elicitation_requests: Vec::new(),
        elicitation_answer: Some("accept"),
        elicitation_completions: Vec::new(),
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
    let Some((base_url, server)) = mock_sequence_server(vec![
        mock_text_stream(),
        mock_tool_call_stream(),
        mock_text_stream(),
    ])
    .await
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
    for (text, expected) in [("first", 22), ("tool round", 36)] {
        let response = peer
            .request(
                "session/prompt",
                json!({
                    "sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": text}]
                }),
            )
            .await;
        assert_eq!(
            response["usage"]["totalTokens"], expected,
            "{text}: {response}"
        );
        assert_eq!(
            response["usage"]["_meta"]["mjolnir.dev/usage-scope"],
            "turn"
        );
    }

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn v2_idle_usage_counts_only_its_own_turn_and_says_so() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        mock_text_stream(),
        mock_tool_call_stream(),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping v2 turn usage test: loopback networking is unavailable");
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
    for (text, expected) in [("first", 22), ("tool round", 36)] {
        peer.request(
            "session/prompt",
            json!({
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": text}]
            }),
        )
        .await;
        let idle = next_v2_idle(&mut peer).await;
        assert_eq!(
            idle["params"]["update"]["usage"]["totalTokens"], expected,
            "{text}: {idle}"
        );
        assert_eq!(
            idle["params"]["update"]["usage"]["_meta"]["mjolnir.dev/usage-scope"], "turn",
            "{text}: {idle}"
        );
    }

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn agents_md_and_resumed_system_prompts_reach_the_model() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let extra = temp.path().join("extra");
    let data_dir = temp.path().join("sessions-data");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join("AGENTS.md"), "user-level instructions\n").unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v1\n").unwrap();
    std::fs::write(extra.join("AGENTS.md"), "additional-root instructions\n").unwrap();
    let Some((base_url, server)) =
        mock_sequence_server(vec![mock_text_stream(), mock_text_stream()]).await
    else {
        eprintln!("skipping AGENTS.md test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "additionalDirectories": [extra],
                "systemPrompt": {
                    "mode": "append",
                    "content": [{"type": "text", "text": "client instructions v1"}]
                }
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;

    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v2\n").unwrap();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": [],
            "additionalDirectories": [extra],
            "systemPrompt": {
                "mode": "append",
                "content": [{"type": "text", "text": "client instructions v2"}]
            }
        }),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let first_prompt = requests[0]["messages"][0]["content"].as_str().unwrap();
    assert!(
        first_prompt.starts_with(HARNESS_PROMPT_START),
        "{first_prompt}"
    );
    for expected in [
        "user-level instructions",
        "workspace instructions v1",
        "additional-root instructions",
        "client instructions v1",
    ] {
        assert!(first_prompt.contains(expected), "{first_prompt}");
    }
    let second_prompt = requests[1]["messages"][0]["content"].as_str().unwrap();
    for expected in [
        "user-level instructions",
        "workspace instructions v2",
        "additional-root instructions",
        "client instructions v2",
    ] {
        assert!(second_prompt.contains(expected), "{second_prompt}");
    }
    assert!(!second_prompt.contains("workspace instructions v1"));
    assert!(!second_prompt.contains("client instructions v1"));
}

#[tokio::test]
async fn a_failed_instruction_refresh_does_not_partially_apply_resume() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let Some((base_url, server)) =
        mock_sequence_server(vec![mock_text_stream(), mock_text_stream()]).await
    else {
        eprintln!("skipping instruction rollback test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": {
                    "mode": "append",
                    "content": [{"type": "text", "text": "client instructions v1"}]
                }
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;

    std::fs::write(workspace.join("AGENTS.md"), [0xff, 0xfe]).unwrap();
    peer.request_error(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": [],
            "systemPrompt": {
                "mode": "append",
                "content": [{"type": "text", "text": "client instructions v2"}]
            }
        }),
    )
    .await;
    std::fs::write(workspace.join("AGENTS.md"), "recovered instructions\n").unwrap();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": []
        }),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let prompt = requests[1]["messages"][0]["content"].as_str().unwrap();
    assert!(prompt.contains("client instructions v1"), "{prompt}");
    assert!(!prompt.contains("client instructions v2"), "{prompt}");
    assert!(prompt.contains("recovered instructions"), "{prompt}");
}

#[tokio::test]
async fn v2_resume_refreshes_agents_md_and_system_prompts() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let data_dir = temp.path().join("sessions-data");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v1\n").unwrap();
    let Some((base_url, server)) =
        mock_sequence_server(vec![mock_text_stream(), mock_text_stream()]).await
    else {
        eprintln!("skipping v2 AGENTS.md test: loopback networking is unavailable");
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
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": {
                    "mode": "append",
                    "content": [{"type": "text", "text": "client instructions v1"}]
                }
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions v2\n").unwrap();
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": [],
            "systemPrompt": {
                "mode": "append",
                "content": [{"type": "text", "text": "client instructions v2"}]
            }
        }),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let first_prompt = requests[0]["messages"][0]["content"].as_str().unwrap();
    assert!(
        first_prompt.starts_with(HARNESS_PROMPT_START),
        "{first_prompt}"
    );
    assert!(first_prompt.contains("workspace instructions v1"));
    assert!(first_prompt.contains("client instructions v1"));
    let second_prompt = requests[1]["messages"][0]["content"].as_str().unwrap();
    assert!(second_prompt.contains("workspace instructions v2"));
    assert!(second_prompt.contains("client instructions v2"));
    assert!(!second_prompt.contains("workspace instructions v1"));
    assert!(!second_prompt.contains("client instructions v1"));
}

/// The opening of the built-in harness prompt.
const HARNESS_PROMPT_START: &str = "You are DeepSeek Harness";

/// The system prompt a recorded model request carried.
fn system_prompt_of(request: &Value) -> &str {
    assert_eq!(request["messages"][0]["role"], "system");
    request["messages"][0]["content"].as_str().unwrap()
}

fn client_system_prompt(mode: &str, text: &str) -> Value {
    json!({"mode": mode, "content": [{"type": "text", "text": text}]})
}

/// An override replaces the harness prompt; `--system-prompt` text and
/// AGENTS.md still follow it, and no "client-provided" section is appended.
fn assert_overridden(prompt: &str, client_text: &str) {
    assert!(prompt.starts_with(&format!("{client_text}\n")), "{prompt}");
    assert!(!prompt.contains(HARNESS_PROMPT_START), "{prompt}");
    assert!(
        !prompt.contains("Client-provided system instructions"),
        "{prompt}"
    );
    assert!(prompt.contains("launch instructions"), "{prompt}");
    assert!(prompt.contains("workspace instructions"), "{prompt}");
}

fn assert_appended(prompt: &str, client_text: &str) {
    assert!(prompt.starts_with(HARNESS_PROMPT_START), "{prompt}");
    assert!(prompt.contains("launch instructions"), "{prompt}");
    assert!(prompt.contains("workspace instructions"), "{prompt}");
    assert!(
        prompt.ends_with(&format!(
            "Client-provided system instructions:\n{client_text}\n"
        )),
        "{prompt}"
    );
}

#[tokio::test]
async fn v1_override_system_prompt_replaces_only_the_harness_prompt() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions\n").unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream(); 4]).await else {
        eprintln!("skipping v1 override test: loopback networking is unavailable");
        return;
    };
    let launch_args = ["--system-prompt", "launch instructions"];
    let mut peer = spawn_peer_with_args(temp.path(), Some(&base_url), &launch_args).await;
    let initialize = peer
        .request("initialize", json!({"protocolVersion": 1}))
        .await;
    assert_eq!(
        initialize["agentCapabilities"]["sessionCapabilities"]["systemPrompt"],
        json!({"mode": {"override": {}}})
    );

    let refused = peer
        .request_error(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v1")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    assert!(refused["data"].as_str().unwrap().contains("_custom"));
    let list = peer.request("session/list", json!({})).await;
    assert!(list["sessions"].as_array().unwrap().is_empty(), "{list}");

    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("override", "client persona v1")
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;
    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;

    // The mode is stored with the session, so a new process that loads it
    // without a system prompt keeps the override. A load with an unsupported
    // mode is refused first.
    let mut peer = spawn_peer_with_args(temp.path(), Some(&base_url), &launch_args).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let refused = peer
        .request_error(
            "session/load",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v2")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    peer.request(
        "session/load",
        json!({"sessionId": session_id, "cwd": workspace, "mcpServers": []}),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;

    // A fork with an unsupported mode creates nothing; one that names no
    // system prompt inherits the override.
    let refused = peer
        .request_error(
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v2")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    let list = peer.request("session/list", json!({})).await;
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1, "{list}");
    let fork = peer
        .request(
            "session/fork",
            json!({"sessionId": session_id, "cwd": workspace, "mcpServers": []}),
        )
        .await;
    let fork_id = fork["sessionId"].as_str().unwrap().to_string();
    // Plan mode still adds its instructions to an overridden prompt.
    peer.request(
        "session/set_mode",
        json!({"sessionId": fork_id, "modeId": "plan"}),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": fork_id, "prompt": [{"type": "text", "text": "third"}]}),
    )
    .await;

    // A resume with an unsupported mode is refused; one with append brings
    // the harness prompt back.
    let refused = peer
        .request_error(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v2")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": [],
            "systemPrompt": client_system_prompt("append", "client persona v2")
        }),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "fourth"}]}),
    )
    .await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    for request in &requests[..3] {
        assert_overridden(system_prompt_of(request), "client persona v1");
    }
    let planned = system_prompt_of(&requests[2]);
    assert!(
        planned.contains("The session is in plan mode."),
        "{planned}"
    );
    assert!(!system_prompt_of(&requests[1]).contains("The session is in plan mode."));
    assert_appended(system_prompt_of(&requests[3]), "client persona v2");
}

#[tokio::test]
async fn v2_override_system_prompt_replaces_only_the_harness_prompt() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions\n").unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream(); 4]).await else {
        eprintln!("skipping v2 override test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer_with_args(
        temp.path(),
        Some(&base_url),
        &["--system-prompt", "launch instructions"],
    )
    .await;
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
        initialize["capabilities"]["session"]["systemPrompt"],
        json!({"mode": {"override": {}}})
    );

    let refused = peer
        .request_error(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v1")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    assert!(refused["data"].as_str().unwrap().contains("_custom"));
    let list = peer.request("session/list", json!({})).await;
    assert!(list["sessions"].as_array().unwrap().is_empty(), "{list}");

    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("override", "client persona v1")
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    // A resume with an unsupported mode changes nothing; one that names no
    // system prompt keeps the stored override.
    let refused = peer
        .request_error(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v2")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    peer.request(
        "session/resume",
        json!({"sessionId": session_id, "cwd": workspace, "mcpServers": []}),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    // A fork with an unsupported mode creates nothing; one that carries a
    // system prompt applies it.
    let refused = peer
        .request_error(
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("_custom", "client persona v2")
            }),
        )
        .await;
    assert_eq!(refused["code"], -32602, "{refused}");
    let list = peer.request("session/list", json!({})).await;
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1, "{list}");
    let fork = peer
        .request(
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("override", "client persona v2")
            }),
        )
        .await;
    let fork_id = fork["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": fork_id, "prompt": [{"type": "text", "text": "third"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    // Resuming with append brings the harness prompt back.
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": workspace,
            "mcpServers": [],
            "systemPrompt": client_system_prompt("append", "client persona v3")
        }),
    )
    .await;
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "fourth"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_overridden(system_prompt_of(&requests[0]), "client persona v1");
    assert_overridden(system_prompt_of(&requests[1]), "client persona v1");
    assert_overridden(system_prompt_of(&requests[2]), "client persona v2");
    assert_appended(system_prompt_of(&requests[3]), "client persona v3");
}

/// The client system prompt, its mode, and the additional directories stored
/// in a session's file.
fn stored_instructions(data_dir: &std::path::Path, session_id: &str) -> (Value, Value, Value) {
    let path = data_dir
        .join("sessions-data")
        .join("sessions")
        .join(format!("{session_id}.json"));
    let record: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    (
        record["system_prompt"].clone(),
        record["system_prompt_mode"].clone(),
        record["additional_directories"].clone(),
    )
}

/// What a session created with an override and no additional directories
/// stores.
fn original_instructions() -> (Value, Value, Value) {
    (json!("client persona v1"), json!("override"), json!([]))
}

/// The failing request reached the MCP connect, rather than being refused
/// earlier.
fn assert_mcp_connect_failed(error: &Value) {
    assert_eq!(error["code"], -32603, "{error}");
}

#[tokio::test]
async fn v1_a_load_or_resume_whose_mcp_server_fails_changes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions\n").unwrap();
    let extra = temp.path().join("extra");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("AGENTS.md"), "additional-root instructions\n").unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream(); 2]).await else {
        eprintln!("skipping v1 MCP failure test: loopback networking is unavailable");
        return;
    };
    let missing_server = json!({
        "name": "missing",
        "command": temp.path().join("missing-mcp-server"),
        "args": [],
        "env": []
    });
    let mut peer = spawn_peer_with_args(
        temp.path(),
        Some(&base_url),
        &["--system-prompt", "launch instructions"],
    )
    .await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let session = peer
        .request(
            "session/new",
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("override", "client persona v1")
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;

    let error = peer
        .request_error(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [missing_server],
                "additionalDirectories": [extra],
                "systemPrompt": client_system_prompt("append", "client persona v2")
            }),
        )
        .await;
    assert_mcp_connect_failed(&error);
    let error = peer
        .request_error(
            "session/load",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [missing_server],
                "additionalDirectories": [extra],
                "systemPrompt": client_system_prompt("append", "client persona v3")
            }),
        )
        .await;
    assert_mcp_connect_failed(&error);
    assert_eq!(
        stored_instructions(temp.path(), &session_id),
        original_instructions()
    );
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    // The prompt saved the live record, so this also covers the resume, which
    // never saves when it fails.
    assert_eq!(
        stored_instructions(temp.path(), &session_id),
        original_instructions()
    );
    let requests = server.await.unwrap();
    for request in &requests {
        let prompt = system_prompt_of(request);
        assert_overridden(prompt, "client persona v1");
        assert!(!prompt.contains("additional-root instructions"), "{prompt}");
    }
}

#[tokio::test]
async fn v2_a_resume_whose_mcp_server_fails_changes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "workspace instructions\n").unwrap();
    let extra = temp.path().join("extra");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("AGENTS.md"), "additional-root instructions\n").unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream(); 2]).await else {
        eprintln!("skipping v2 MCP failure test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer_with_args(
        temp.path(),
        Some(&base_url),
        &["--system-prompt", "launch instructions"],
    )
    .await;
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
            json!({
                "cwd": workspace,
                "mcpServers": [],
                "systemPrompt": client_system_prompt("override", "client persona v1")
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "first"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    let error = peer
        .request_error(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": [{
                    "type": "stdio",
                    "name": "missing",
                    "command": temp.path().join("missing-mcp-server")
                }],
                "additionalDirectories": [extra],
                "systemPrompt": client_system_prompt("append", "client persona v2")
            }),
        )
        .await;
    assert_mcp_connect_failed(&error);
    assert_eq!(
        stored_instructions(temp.path(), &session_id),
        original_instructions()
    );
    peer.request(
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "second"}]}),
    )
    .await;
    next_v2_idle(&mut peer).await;

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    // The prompt saved the live record, so this also covers the in-memory
    // session the failed resume must not have changed.
    assert_eq!(
        stored_instructions(temp.path(), &session_id),
        original_instructions()
    );
    let requests = server.await.unwrap();
    for request in &requests {
        let prompt = system_prompt_of(request);
        assert_overridden(prompt, "client persona v1");
        assert!(!prompt.contains("additional-root instructions"), "{prompt}");
    }
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
async fn a_blank_api_key_fails_locally_without_calling_the_model() {
    let Some((base_url, mut authorizations)) = mock_authorization_recording_server().await else {
        eprintln!("skipping blank API key test: loopback networking is unavailable");
        return;
    };
    let cases: [(&str, Option<&str>, &[&str]); 3] = [
        ("empty DEEPSEEK_API_KEY", Some(""), &[]),
        ("blank DEEPSEEK_API_KEY", Some("   "), &[]),
        ("blank --api-key", None, &["--api-key", "  "]),
    ];
    for (case, api_key, extra_args) in cases {
        let temp = tempfile::tempdir().unwrap();
        let mut peer =
            spawn_peer_with_api_key(temp.path(), Some(&base_url), api_key, extra_args).await;
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
        assert_eq!(error["code"], -32000, "{case}: {error}");
        assert_eq!(
            error["data"],
            "Set DEEPSEEK_API_KEY, pass --api-key, or configure an Authorization provider header",
            "{case}: {error}"
        );
        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
        if let Ok(authorization) = authorizations.try_recv() {
            panic!("{case}: the model was called with Authorization {authorization:?}");
        }
    }

    // A non-blank key is sent unchanged, and a provider Authorization header
    // still stands in for a blank key.
    let cases = [
        (
            "non-blank DEEPSEEK_API_KEY",
            "test-key",
            None,
            "Bearer test-key",
        ),
        (
            "provider Authorization header",
            "   ",
            Some("Bearer provider-key"),
            "Bearer provider-key",
        ),
    ];
    for (case, api_key, provider_authorization, expected) in cases {
        let temp = tempfile::tempdir().unwrap();
        let mut peer =
            spawn_peer_with_api_key(temp.path(), Some(&base_url), Some(api_key), &[]).await;
        peer.request("initialize", json!({"protocolVersion": 1}))
            .await;
        if let Some(authorization) = provider_authorization {
            peer.request(
                "providers/set",
                json!({
                    "providerId": "deepseek",
                    "apiType": "openai",
                    "baseUrl": base_url,
                    "headers": {"Authorization": authorization}
                }),
            )
            .await;
        }
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
                    "prompt": [{"type": "text", "text": "hello"}]
                }),
            )
            .await;
        assert_eq!(response["stopReason"], "end_turn", "{case}");
        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
        assert_eq!(
            authorizations.try_recv().ok(),
            Some(Some(expected.to_string())),
            "{case}"
        );
    }
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
    let Some((mcp_url, mcp_server)) = mock_http_mcp_server(5, None).await else {
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
    let mcp_requests = served_mcp_requests(mcp_server).await;
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

/// A model turn that asks the user to open an authorization URL.
fn ask_url_stream(id: &str, elicitation_id: &str) -> String {
    tool_call_stream(
        id,
        "ask_user",
        json!({
            "message": "Authorize repository access",
            "url": "https://auth.example.com/connect",
            "elicitation_id": elicitation_id
        }),
    )
}

/// A model turn that reports a URL interaction finished.
fn complete_url_stream(id: &str, elicitation_id: &str) -> String {
    tool_call_stream(
        id,
        "complete_url_elicitation",
        json!({"elicitation_id": elicitation_id}),
    )
}

/// The last update the peer received for a tool call.
fn final_tool_update<'a>(peer: &'a Peer, tool_call_id: &str) -> &'a Value {
    peer.updates
        .iter()
        .map(|update| &update["params"]["update"])
        .rfind(|update| update["toolCallId"] == tool_call_id)
        .unwrap_or_else(|| panic!("no update for tool call {tool_call_id}"))
}

/// What a model request said a tool call returned.
fn tool_message<'a>(request: &'a Value, tool_call_id: &str) -> &'a str {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["tool_call_id"] == tool_call_id)
        .and_then(|message| message["content"].as_str())
        .unwrap_or_else(|| panic!("no tool message for {tool_call_id}"))
}

fn offered_tools(request: &Value) -> Vec<&str> {
    request["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect()
}

/// `elicitation/complete` went out while the completing tool call ran: after
/// the call was announced and before it finished.
fn assert_completion_sent_during(peer: &Peer, completion: usize, tool_call_id: &str) {
    let at = peer.elicitation_completions[completion].1;
    assert!(
        peer.updates[..at]
            .iter()
            .any(|update| update["params"]["update"]["toolCallId"] == tool_call_id)
    );
    assert!(peer.updates[at..].iter().any(|update| {
        update["params"]["update"]["toolCallId"] == tool_call_id
            && update["params"]["update"]["status"] == "completed"
    }));
}

#[tokio::test]
async fn v1_reports_an_accepted_url_elicitation_complete_once() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        ask_url_stream("call-ask", "oauth-1"),
        // Reusing an ID that is still outstanding would break its uniqueness
        // on the connection.
        ask_url_stream("call-ask-again", "oauth-1"),
        mock_text_stream(),
        complete_url_stream("call-complete", "oauth-1"),
        complete_url_stream("call-complete-again", "oauth-1"),
        complete_url_stream("call-complete-unknown", "never-requested"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"elicitation": {"form": {}, "url": {}}}
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
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "Connect the repository"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(peer.elicitation_requests.len(), 1);
    let request = &peer.elicitation_requests[0]["params"];
    assert_eq!(request["mode"], "url");
    assert_eq!(request["elicitationId"], "oauth-1");
    assert_eq!(request["sessionId"], session_id.as_str());
    assert_eq!(request["toolCallId"], "call-ask");
    let asked = final_tool_update(&peer, "call-ask");
    assert_eq!(asked["status"], "completed");
    assert_eq!(asked["rawOutput"]["elicitation_id"], "oauth-1");
    assert_eq!(
        final_tool_update(&peer, "call-ask-again")["status"],
        "failed"
    );
    // Accepting is consent, not completion.
    assert!(peer.elicitation_completions.is_empty());

    // The model checks the interaction finished in a later turn and reports
    // it, in plan mode, which keeps the tool and asks no permission. Loading
    // the session on the same connection keeps the pending elicitation.
    peer.request(
        "session/load",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": []
        }),
    )
    .await;
    peer.request(
        "session/set_mode",
        json!({"sessionId": session_id, "modeId": "plan"}),
    )
    .await;
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "I finished authorizing"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(peer.elicitation_completions.len(), 1);
    let (completion, _) = &peer.elicitation_completions[0];
    assert!(completion.get("id").is_none(), "{completion}");
    assert_eq!(completion["params"]["elicitationId"], "oauth-1");
    assert_completion_sent_during(&peer, 0, "call-complete");
    assert_eq!(
        final_tool_update(&peer, "call-complete")["status"],
        "completed"
    );
    for refused in ["call-complete-again", "call-complete-unknown"] {
        assert_eq!(final_tool_update(&peer, refused)["status"], "failed");
    }
    assert!(peer.permission_requests.is_empty());
    // Replay shows the tool call but does not report the completion again.
    peer.request(
        "session/load",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": []
        }),
    )
    .await;
    assert_eq!(peer.elicitation_completions.len(), 1);

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 7);
    for request in [&requests[0], &requests[3]] {
        let offered = offered_tools(request);
        assert!(offered.contains(&"ask_user"), "{offered:?}");
        assert!(offered.contains(&"complete_url_elicitation"), "{offered:?}");
    }
    // The model is told the ID, that completion is unverified, and why a
    // reused ID or an unknown completion was refused.
    let accepted = tool_message(&requests[1], "call-ask");
    assert!(
        accepted.contains("oauth-1")
            && accepted.contains("not verified")
            && accepted.contains("complete_url_elicitation"),
        "{accepted}"
    );
    let reused = tool_message(&requests[2], "call-ask-again");
    assert!(reused.contains("still in use"), "{reused}");
    let unknown = tool_message(&requests[6], "call-complete-unknown");
    assert!(unknown.contains("Nothing was reported"), "{unknown}");
}

#[tokio::test]
async fn v2_reports_an_accepted_url_elicitation_complete_once() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        ask_url_stream("call-ask", "oauth-2"),
        // A retry that reuses the outstanding ID is refused, but the turn goes
        // on and the model learns why.
        ask_url_stream("call-ask-again", "oauth-2"),
        mock_text_stream(),
        complete_url_stream("call-complete", "oauth-2"),
        complete_url_stream("call-complete-again", "oauth-2"),
        complete_url_stream("call-complete-unknown", "never-requested"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping v2 URL elicitation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {"elicitation": {"url": {}}}
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
    for text in ["Connect the repository", "I finished authorizing"] {
        peer.request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": text}]}),
        )
        .await;
        let idle = next_v2_idle(&mut peer).await;
        assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
        // Resuming on the same connection keeps the pending elicitation.
        peer.request(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": temp.path().to_string_lossy(),
                "mcpServers": []
            }),
        )
        .await;
    }
    assert_eq!(peer.elicitation_requests.len(), 1);
    let request = &peer.elicitation_requests[0]["params"];
    assert_eq!(request["mode"], "url");
    assert_eq!(request["elicitationId"], "oauth-2");
    assert_eq!(request["toolCallId"], "call-ask");
    assert_eq!(
        final_tool_update(&peer, "call-ask")["rawOutput"]["elicitation_id"],
        "oauth-2"
    );
    assert_eq!(
        final_tool_update(&peer, "call-ask-again")["status"],
        "failed"
    );
    // Only the elicitation that was sent waited on the user; the refused one
    // never reported `requires_action`.
    assert_eq!(
        peer.updates
            .iter()
            .filter(|update| update["params"]["update"]["state"] == "requires_action")
            .count(),
        1
    );
    assert_eq!(peer.elicitation_completions.len(), 1);
    let (completion, _) = &peer.elicitation_completions[0];
    assert!(completion.get("id").is_none(), "{completion}");
    assert_eq!(completion["params"]["elicitationId"], "oauth-2");
    assert_completion_sent_during(&peer, 0, "call-complete");
    let completed = final_tool_update(&peer, "call-complete");
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["rawOutput"]["completed"], true);
    for refused in ["call-complete-again", "call-complete-unknown"] {
        assert_eq!(final_tool_update(&peer, refused)["status"], "failed");
    }
    // Reporting completion is not a foreground state change.
    let completing = peer
        .updates
        .iter()
        .position(|update| update["params"]["update"]["toolCallId"] == "call-complete")
        .unwrap();
    assert!(
        !peer.updates[completing..]
            .iter()
            .any(|update| update["params"]["update"]["state"] == "requires_action")
    );
    assert!(peer.permission_requests.is_empty());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 7);
    let offered = offered_tools(&requests[0]);
    assert!(offered.contains(&"complete_url_elicitation"), "{offered:?}");
    let accepted = tool_message(&requests[1], "call-ask");
    assert!(accepted.contains("oauth-2"), "{accepted}");
    let reused = tool_message(&requests[2], "call-ask-again");
    assert!(
        reused.contains("still in use") && reused.contains("omit elicitation_id"),
        "{reused}"
    );
}

#[tokio::test]
async fn declined_and_closed_url_elicitations_are_never_reported_complete() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        ask_url_stream("call-ask-declined", "oauth-3"),
        complete_url_stream("call-complete-declined", "oauth-3"),
        mock_text_stream(),
        ask_url_stream("call-ask", "oauth-4"),
        mock_text_stream(),
        complete_url_stream("call-complete-after-close", "oauth-4"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation close test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {"elicitation": {"url": {}}}
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
    let prompt = json!({
        "sessionId": session_id,
        "prompt": [{"type": "text", "text": "Connect the repository"}]
    });

    peer.elicitation_answer = Some("decline");
    peer.request("session/prompt", prompt.clone()).await;
    next_v2_idle(&mut peer).await;
    assert_eq!(
        final_tool_update(&peer, "call-complete-declined")["status"],
        "failed"
    );

    // Closing the session drops what it was waiting for: the connection never
    // learns whether the interaction finished.
    peer.elicitation_answer = Some("accept");
    peer.request("session/prompt", prompt.clone()).await;
    next_v2_idle(&mut peer).await;
    peer.request("session/close", json!({"sessionId": session_id}))
        .await;
    peer.request(
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": []
        }),
    )
    .await;
    peer.request("session/prompt", prompt).await;
    next_v2_idle(&mut peer).await;
    assert_eq!(
        final_tool_update(&peer, "call-complete-after-close")["status"],
        "failed"
    );

    assert_eq!(peer.elicitation_requests.len(), 2);
    assert!(peer.elicitation_completions.is_empty());
    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 7);
}

#[tokio::test]
async fn an_accepted_url_elicitation_survives_session_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_stalling_sequence_server(vec![
        Some(ask_url_stream("call-ask", "oauth-6")),
        // The model is still answering when the user cancels.
        None,
        Some(complete_url_stream("call-complete", "oauth-6")),
        Some(mock_text_stream()),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation cancel test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"elicitation": {"url": {}}}
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
    let prompt_id = peer.next_id;
    peer.next_id += 1;
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": prompt_id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "Connect the repository"}]
        }
    }))
    .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message.get("id").and_then(Value::as_u64) == Some(prompt_id) {
                return message;
            }
            if message["params"]["update"]["content"]["text"] == "working" {
                peer.write(json!({
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": {"sessionId": session_id}
                }))
                .await;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("cancellation should complete promptly");
    assert_eq!(response["result"]["stopReason"], "cancelled");
    assert_eq!(final_tool_update(&peer, "call-ask")["status"], "completed");

    // The user may still finish in the browser, so a later turn can report it.
    let response = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "I finished authorizing"}]
            }),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(
        final_tool_update(&peer, "call-complete")["status"],
        "completed"
    );
    assert_eq!(peer.elicitation_completions.len(), 1);
    assert_eq!(
        peer.elicitation_completions[0].0["params"]["elicitationId"],
        "oauth-6"
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 4);
}

#[tokio::test]
async fn a_url_elicitation_cancelled_before_its_answer_keeps_its_id_reserved() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        ask_url_stream("call-ask", "oauth-7"),
        ask_url_stream("call-ask-again", "oauth-7"),
        complete_url_stream("call-complete", "oauth-7"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation reservation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {"elicitation": {"url": {}}}
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
    let prompt = json!({
        "sessionId": session_id,
        "prompt": [{"type": "text", "text": "Connect the repository"}]
    });

    // The user cancels the turn while the Client is still showing the request.
    peer.elicitation_answer = None;
    peer.request("session/prompt", prompt.clone()).await;
    let held = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message["method"] == "elicitation/create" {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("the agent should request the URL elicitation");
    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");
    // The Client's accept was already on its way and arrives too late.
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": held["id"],
        "result": {"action": "accept"}
    }))
    .await;

    // The ID may be in use in the user's browser, so it is neither reused nor
    // completed.
    peer.elicitation_answer = Some("accept");
    peer.request("session/prompt", prompt).await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert_eq!(peer.elicitation_requests.len(), 1);
    for refused in ["call-ask-again", "call-complete"] {
        assert_eq!(final_tool_update(&peer, refused)["status"], "failed");
    }
    assert!(peer.elicitation_completions.is_empty());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    let reused = tool_message(&requests[2], "call-ask-again");
    assert!(reused.contains("still in use"), "{reused}");
}

#[tokio::test]
async fn a_client_cancelling_an_elicitation_fails_only_that_tool_call() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, server)) = mock_sequence_server(vec![
            ask_url_stream("call-ask", "oauth-9"),
            // The Client's cancel answered the request, so the ID is free.
            ask_url_stream("call-ask-again", "oauth-9"),
            mock_text_stream(),
        ])
        .await
        else {
            eprintln!("skipping client-cancel test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        let initialize = if version == 1 {
            json!({"protocolVersion": 1, "clientCapabilities": {"elicitation": {"url": {}}}})
        } else {
            json!({
                "protocolVersion": 2,
                "info": {"name": "v2-test-client", "version": "1.0.0"},
                "capabilities": {"elicitation": {"url": {}}}
            })
        };
        peer.request("initialize", initialize).await;
        let session = peer
            .request(
                "session/new",
                json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
            )
            .await;
        let prompt_id = peer.next_id;
        peer.next_id += 1;
        peer.write(json!({
            "jsonrpc": "2.0",
            "id": prompt_id,
            "method": "session/prompt",
            "params": {
                "sessionId": session["sessionId"],
                "prompt": [{"type": "text", "text": "Connect the repository"}]
            }
        }))
        .await;
        peer.elicitation_answer = None;
        // v2 accepts the prompt first; an idle before that is the new
        // session's initial state.
        let mut accepted = false;
        let stop_reason = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let message = peer.next_message().await;
                if message["method"] == "elicitation/create" {
                    // The Client dismisses its own dialog: no session/cancel
                    // was sent, so the turn must go on.
                    peer.write(json!({
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "error": {"code": -32800, "message": "Request cancelled"}
                    }))
                    .await;
                    peer.elicitation_answer = Some("accept");
                    continue;
                }
                if message["id"] == prompt_id {
                    if version == 1 {
                        return message["result"]["stopReason"].clone();
                    }
                    accepted = true;
                    continue;
                }
                if accepted && message["params"]["update"]["state"] == "idle" {
                    return message["params"]["update"]["stopReason"].clone();
                }
                peer.updates.push(message);
            }
        })
        .await
        .expect("the turn should finish");
        assert_eq!(stop_reason, "end_turn", "v{version}");
        assert_eq!(
            final_tool_update(&peer, "call-ask")["status"],
            "failed",
            "v{version}"
        );
        assert_eq!(
            final_tool_update(&peer, "call-ask-again")["status"],
            "completed",
            "v{version}"
        );
        assert_eq!(peer.elicitation_requests.len(), 2, "v{version}");
        assert_eq!(
            peer.elicitation_requests[1]["params"]["elicitationId"], "oauth-9",
            "v{version}"
        );

        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3, "v{version}");
        let cancelled = tool_message(&requests[1], "call-ask");
        assert!(
            cancelled.contains("the client cancelled the request"),
            "v{version}: {cancelled}"
        );
    }
}

#[tokio::test]
async fn a_client_cancelling_a_permission_request_fails_only_that_tool_call() {
    let errors = [
        // The Client dismisses its own dialog: no session/cancel was sent, so
        // the turn must go on.
        (
            json!({"code": -32800, "message": "Request cancelled"}),
            "the client cancelled the permission request",
        ),
        (
            json!({
                "code": -32603,
                "message": "Internal error",
                "data": "permission dialog unavailable"
            }),
            "permission dialog unavailable",
        ),
    ];
    for version in [1, 2] {
        for (error, reason) in &errors {
            let case = format!("v{version} {}", error["code"]);
            let temp = tempfile::tempdir().unwrap();
            let Some((base_url, server)) =
                mock_sequence_server(vec![mock_tool_call_stream(), mock_text_stream()]).await
            else {
                eprintln!("skipping permission error test: loopback networking is unavailable");
                return;
            };
            let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
            let initialize = if version == 1 {
                json!({"protocolVersion": 1, "clientCapabilities": {"fs": {}, "terminal": false}})
            } else {
                json!({
                    "protocolVersion": 2,
                    "info": {"name": "v2-test-client", "version": "1.0.0"},
                    "capabilities": {}
                })
            };
            peer.request("initialize", initialize).await;
            let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
            peer.permission_error = Some(error.clone());
            let response = peer
                .request(
                    "session/prompt",
                    json!({
                        "sessionId": session_id,
                        "prompt": [{"type": "text", "text": "Write sample.txt"}]
                    }),
                )
                .await;
            let stop_reason = if version == 1 {
                response["stopReason"].clone()
            } else {
                next_v2_idle(&mut peer).await["params"]["update"]["stopReason"].clone()
            };
            assert_eq!(stop_reason, "end_turn", "{case}");
            assert_eq!(peer.permission_requests.len(), 1, "{case}");
            assert_eq!(
                final_tool_update(&peer, "call-1")["status"],
                "failed",
                "{case}"
            );
            assert!(!temp.path().join("sample.txt").exists(), "{case}");
            if version == 2 {
                // Progress resumed after the request, as it does for a denial.
                let states = peer
                    .updates
                    .iter()
                    .map(|update| &update["params"]["update"])
                    .filter(|update| update["sessionUpdate"] == "state_update")
                    .map(|update| update["state"].clone())
                    .collect::<Vec<_>>();
                assert!(
                    states
                        .windows(2)
                        .any(|pair| pair[0] == "requires_action" && pair[1] == "running"),
                    "{case}: {states:?}"
                );
            }

            drop(peer.stdin);
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
            let requests = server.await.unwrap();
            assert_eq!(requests.len(), 2, "{case}");
            let failed = tool_message(&requests[1], "call-1");
            assert!(failed.contains(reason), "{case}: {failed}");
            assert!(failed.contains("did not run"), "{case}: {failed}");

            let saved: Value = serde_json::from_slice(
                &std::fs::read(
                    temp.path()
                        .join("sessions-data/sessions")
                        .join(format!("{session_id}.json")),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                saved["tool_outcomes"]["call-1"]["status"], "failed",
                "{case}: {saved}"
            );
        }
    }
}

#[tokio::test]
async fn session_cancel_during_a_permission_request_still_ends_the_turn_cancelled() {
    // A Client must answer a pending permission request `cancelled` when it
    // sends session/cancel. One that answers -32800 instead still cancels the
    // turn, because the turn's own cancellation came first.
    let answers = [
        json!({"result": {"outcome": {"outcome": "cancelled"}}}),
        json!({"error": {"code": -32800, "message": "Request cancelled"}}),
    ];
    for version in [1, 2] {
        for answer in &answers {
            let case = format!("v{version} {answer}");
            let temp = tempfile::tempdir().unwrap();
            let Some((base_url, server)) =
                mock_sequence_server(vec![mock_tool_call_stream()]).await
            else {
                eprintln!("skipping permission cancel test: loopback networking is unavailable");
                return;
            };
            let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
            let initialize = if version == 1 {
                json!({"protocolVersion": 1, "clientCapabilities": {"fs": {}, "terminal": false}})
            } else {
                json!({
                    "protocolVersion": 2,
                    "info": {"name": "v2-test-client", "version": "1.0.0"},
                    "capabilities": {}
                })
            };
            peer.request("initialize", initialize).await;
            let session_id = peer_new_session(&mut peer, &temp.path().to_string_lossy()).await;
            let prompt_id = peer.next_id;
            peer.next_id += 1;
            peer.write(json!({
                "jsonrpc": "2.0",
                "id": prompt_id,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": "Write sample.txt"}]
                }
            }))
            .await;
            let mut permission_asked = false;
            // v2 accepts the prompt first; an idle before that is the new
            // session's initial state.
            let mut accepted = false;
            let stop_reason = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let message = peer.read_message().await;
                    if message["method"] == "session/request_permission" {
                        permission_asked = true;
                        let mut reply = answer.as_object().unwrap().clone();
                        reply.insert("jsonrpc".to_string(), json!("2.0"));
                        reply.insert("id".to_string(), message["id"].clone());
                        // One write, so the agent reads session/cancel first.
                        peer.write_together(&[
                            json!({
                                "jsonrpc": "2.0",
                                "method": "session/cancel",
                                "params": {"sessionId": session_id}
                            }),
                            Value::Object(reply),
                        ])
                        .await;
                        continue;
                    }
                    // The agent may withdraw the request it no longer needs.
                    if message["method"] == "$/cancel_request" {
                        continue;
                    }
                    if message["id"] == prompt_id {
                        if version == 1 {
                            return message["result"]["stopReason"].clone();
                        }
                        accepted = true;
                        continue;
                    }
                    if accepted && message["params"]["update"]["state"] == "idle" {
                        return message["params"]["update"]["stopReason"].clone();
                    }
                    peer.updates.push(message);
                }
            })
            .await
            .expect("the turn should finish");
            assert!(permission_asked, "{case}");
            assert_eq!(stop_reason, "cancelled", "{case}");
            assert_eq!(
                final_tool_update(&peer, "call-1")["status"],
                if version == 1 { "failed" } else { "cancelled" },
                "{case}"
            );
            assert!(!temp.path().join("sample.txt").exists(), "{case}");

            drop(peer.stdin);
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
            assert_eq!(server.await.unwrap().len(), 1, "{case}");
        }
    }
}

#[tokio::test]
async fn a_fork_does_not_inherit_pending_url_elicitations() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        ask_url_stream("call-ask", "oauth-8"),
        mock_text_stream(),
        complete_url_stream("call-complete-fork", "oauth-8"),
        mock_text_stream(),
        complete_url_stream("call-complete", "oauth-8"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation fork test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"elicitation": {"url": {}}}
        }),
    )
    .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let session = peer
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let prompt = |session_id: &str| {
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "Connect the repository"}]
        })
    };
    peer.request("session/prompt", prompt(&session_id)).await;
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

    // The fork has the transcript with the ID, but not the pending entry.
    peer.request("session/prompt", prompt(&fork_id)).await;
    assert_eq!(
        final_tool_update(&peer, "call-complete-fork")["status"],
        "failed"
    );
    assert!(peer.elicitation_completions.is_empty());
    // The session that asked can still report it.
    peer.request("session/prompt", prompt(&session_id)).await;
    assert_eq!(
        final_tool_update(&peer, "call-complete")["status"],
        "completed"
    );
    assert_eq!(peer.elicitation_completions.len(), 1);
    assert_eq!(
        peer.elicitation_completions[0].0["params"]["elicitationId"],
        "oauth-8"
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 6);
    assert!(tool_message(&requests[2], "call-ask").contains("oauth-8"));
}

#[tokio::test]
async fn url_elicitation_completion_is_offered_only_with_url_support() {
    let temp = tempfile::tempdir().unwrap();
    // The v1 client supports forms only; the model calls the tool anyway.
    let Some((base_url, server)) = mock_sequence_server(vec![
        complete_url_stream("call-complete", "oauth-5"),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping URL elicitation offer test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"elicitation": {"form": {}}}
        }),
    )
    .await;
    let response = peer_prompt(&mut peer, temp.path(), "Connect the repository").await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(
        final_tool_update(&peer, "call-complete")["status"],
        "failed"
    );
    assert!(peer.elicitation_completions.is_empty());
    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let offered = offered_tools(&requests[0]);
    assert!(offered.contains(&"ask_user"), "{offered:?}");
    assert!(
        !offered.contains(&"complete_url_elicitation"),
        "{offered:?}"
    );

    // Nor is it offered to a v2 client that supports forms only.
    let Some((base_url, server)) = mock_sequence_server(vec![mock_text_stream()]).await else {
        return;
    };
    let v2_temp = tempfile::tempdir().unwrap();
    let mut peer = spawn_peer(v2_temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 2,
            "info": {"name": "v2-test-client", "version": "1.0.0"},
            "capabilities": {"elicitation": {"form": {}}}
        }),
    )
    .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": v2_temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session["sessionId"],
            "prompt": [{"type": "text", "text": "Connect the repository"}]
        }),
    )
    .await;
    next_v2_idle(&mut peer).await;
    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    let offered = offered_tools(&requests[0]);
    assert!(offered.contains(&"ask_user"), "{offered:?}");
    assert!(
        !offered.contains(&"complete_url_elicitation"),
        "{offered:?}"
    );
}

#[tokio::test]
async fn resume_and_load_connect_mcp_servers_after_a_cancel() {
    let temp = tempfile::tempdir().unwrap();
    // Three connections send initialize, notifications/initialized, and
    // tools/list each, and the cancelled turn refreshes tools/list once.
    let Some((mcp_url, mcp_server)) = mock_http_mcp_server(10, None).await else {
        eprintln!("skipping MCP after cancel test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping MCP after cancel test: loopback networking is unavailable");
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
    let lifecycle = json!({
        "sessionId": session_id,
        "cwd": cwd,
        "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}],
        "additionalDirectories": []
    });
    let cancel = json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    });

    peer.write(cancel.clone()).await;
    let resumed = peer.request("session/resume", lifecycle.clone()).await;
    assert!(resumed["modes"].is_object());

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
                peer.write(cancel.clone()).await;
                cancellation_sent = true;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("cancellation should complete promptly");
    assert_eq!(response["result"]["stopReason"], "cancelled");

    let resumed = peer.request("session/resume", lifecycle.clone()).await;
    assert!(resumed["modes"].is_object());
    let loaded = peer.request("session/load", lifecycle).await;
    assert!(loaded["modes"].is_object());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    let mcp_requests = served_mcp_requests(mcp_server).await;
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|request| request["method"] == "initialize")
            .count(),
        3,
        "MCP requests: {mcp_requests:?}"
    );
}

#[tokio::test]
async fn v2_resume_connects_mcp_servers_after_a_cancel() {
    let temp = tempfile::tempdir().unwrap();
    // Two connections send initialize, notifications/initialized, and
    // tools/list each, and the cancelled turn refreshes tools/list once.
    let Some((mcp_url, mcp_server)) = mock_http_mcp_server(7, None).await else {
        eprintln!("skipping v2 MCP after cancel test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping v2 MCP after cancel test: loopback networking is unavailable");
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
    let resume = json!({
        "sessionId": session_id,
        "cwd": cwd,
        "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}],
        "additionalDirectories": []
    });
    let cancel = json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    });

    peer.write(cancel.clone()).await;
    let resumed = peer.request("session/resume", resume.clone()).await;
    assert!(resumed["configOptions"].is_array());

    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "keep working"}]
        }),
    )
    .await;
    let mut cancellation_sent = false;
    let idle = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if !cancellation_sent
                && message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            {
                peer.write(cancel.clone()).await;
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
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");

    let resumed = peer.request("session/resume", resume).await;
    assert!(resumed["configOptions"].is_array());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    let mcp_requests = served_mcp_requests(mcp_server).await;
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|request| request["method"] == "initialize")
            .count(),
        2,
        "MCP requests: {mcp_requests:?}"
    );
}

/// A v1 stdio MCP server declaration that starts, writes its PID to
/// `pid_file`, and never answers.
#[cfg(unix)]
fn silent_stdio_mcp_server(pid_file: &std::path::Path) -> Value {
    json!({
        "name": "silent",
        "command": "/bin/sh",
        "args": ["-c", "echo $$ > \"$1\"; exec sleep 60", "sh", pid_file],
        "env": []
    })
}

/// Sends a lifecycle request that declares a silent MCP server to a peer
/// started with a one-second connect timeout. It must fail promptly with an
/// error naming the server and the setting, with the server process gone.
#[cfg(unix)]
async fn assert_silent_mcp_server_fails(
    peer: &mut Peer,
    method: &str,
    params: Value,
    pid_file: &std::path::Path,
) {
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        peer.request_error(method, params),
    )
    .await
    .unwrap_or_else(|_| panic!("{method} hung despite the one-second MCP connect timeout"));
    let message = error["data"].as_str().unwrap_or_default();
    assert!(message.contains("MCP server `silent`"), "{method}: {error}");
    assert!(
        message.contains("DSH_ACP_MCP_CONNECT_TIMEOUT_SECS"),
        "{method}: {error}"
    );
    let pid = std::fs::read_to_string(pid_file).expect("the MCP server should have started");
    // `kill -0` also succeeds for an exited process nobody has reaped.
    let running = std::process::Command::new("/bin/sh")
        .args(["-c", "kill -0 \"$1\" 2>/dev/null", "sh", pid.trim()])
        .status()
        .unwrap()
        .success();
    assert!(
        !running,
        "{method}: MCP server {} is still running",
        pid.trim()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_stdio_mcp_server_that_never_answers_fails_session_new_in_time() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer =
        spawn_peer_with_args(temp.path(), None, &["--mcp-connect-timeout-secs", "1"]).await;
    peer.request("initialize", json!({"protocolVersion": 1}))
        .await;
    let cwd = temp.path().to_string_lossy().to_string();
    let pid_file = temp.path().join("mcp.pid");
    assert_silent_mcp_server_fails(
        &mut peer,
        "session/new",
        json!({
            "cwd": cwd,
            "mcpServers": [silent_stdio_mcp_server(&pid_file)],
            "additionalDirectories": []
        }),
        &pid_file,
    )
    .await;

    // The connection still answers, and the failed session was not kept.
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let list = peer.request("session/list", json!({"cwd": cwd})).await;
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(list["sessions"][0]["sessionId"], session["sessionId"]);

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
}

#[cfg(unix)]
#[tokio::test]
async fn v2_a_stdio_mcp_server_that_never_answers_fails_session_setup_in_time() {
    let temp = tempfile::tempdir().unwrap();
    let mut peer =
        spawn_peer_with_args(temp.path(), None, &["--mcp-connect-timeout-secs", "1"]).await;
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
    let silent = |pid_file: &std::path::Path| {
        let mut server = silent_stdio_mcp_server(pid_file);
        server["type"] = json!("stdio");
        server
    };
    let new_pid_file = temp.path().join("new.pid");
    assert_silent_mcp_server_fails(
        &mut peer,
        "session/new",
        json!({
            "cwd": cwd,
            "mcpServers": [silent(&new_pid_file)],
            "additionalDirectories": []
        }),
        &new_pid_file,
    )
    .await;

    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let resume_pid_file = temp.path().join("resume.pid");
    assert_silent_mcp_server_fails(
        &mut peer,
        "session/resume",
        json!({
            "sessionId": session_id,
            "cwd": cwd,
            "mcpServers": [silent(&resume_pid_file)],
            "additionalDirectories": []
        }),
        &resume_pid_file,
    )
    .await;

    // The connection still answers, and the session can still be resumed.
    let resumed = peer
        .request(
            "session/resume",
            json!({
                "sessionId": session_id,
                "cwd": cwd,
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    assert!(resumed["configOptions"].is_array());

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
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

/// Start a v2 session with no MCP servers and send it `text`, returning once
/// the turn is idle.
async fn v2_prompt_until_idle(peer: &mut Peer, cwd: &std::path::Path, text: &str) -> Value {
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
            json!({"cwd": cwd.to_string_lossy(), "mcpServers": []}),
        )
        .await;
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session["sessionId"],
            "prompt": [{"type": "text", "text": text}]
        }),
    )
    .await;
    next_v2_idle(peer).await
}

/// `updates` report `call-bad-plan` as failed with its reason and carry
/// exactly one plan, the valid one.
fn assert_refused_plan(updates: &[Value]) {
    let updates: Vec<&Value> = updates
        .iter()
        .map(|update| &update["params"]["update"])
        .collect();
    let refused = updates
        .iter()
        .rfind(|update| update["toolCallId"] == "call-bad-plan")
        .expect("an update for the refused plan");
    assert_eq!(refused["status"], "failed", "{refused}");
    assert_eq!(refused["rawOutput"]["error"], "plan entry requires content");
    let plans: Vec<&&Value> = updates
        .iter()
        .filter(|update| update["sessionUpdate"] == "plan_update")
        .collect();
    assert_eq!(plans.len(), 1, "{plans:?}");
    let entries = plans[0]["plan"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0]["content"], "Read the code");
    assert_eq!(entries[0]["status"], "in_progress");
}

#[tokio::test]
async fn v2_an_invalid_plan_fails_only_that_tool_call() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![
        // An entry without content is refused, but the turn goes on and the
        // model learns why.
        tool_call_stream(
            "call-bad-plan",
            "update_plan",
            json!({"entries": [{"step": "Read the code", "status": "in_progress"}]}),
        ),
        tool_call_stream(
            "call-plan",
            "update_plan",
            json!({"entries": [{"content": "Read the code", "status": "in_progress"}]}),
        ),
        mock_text_stream(),
    ])
    .await
    else {
        eprintln!("skipping v2 invalid plan test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    let idle = v2_prompt_until_idle(&mut peer, temp.path(), "Plan the work").await;
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert_eq!(final_tool_update(&peer, "call-plan")["status"], "completed");
    // Live and on replay, the refused call is failed with its reason and only
    // the valid plan reaches the Client.
    assert_refused_plan(&peer.updates);
    let replay_start = peer.updates.len();
    peer.request(
        "session/resume",
        json!({
            "sessionId": idle["params"]["sessionId"],
            "cwd": temp.path().to_string_lossy(),
            "mcpServers": [],
            "replayFrom": {"type": "start"}
        }),
    )
    .await;
    assert_refused_plan(&peer.updates[replay_start..]);

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    let refused = tool_message(&requests[1], "call-bad-plan");
    assert!(refused.contains("plan entry requires content"), "{refused}");
}

#[tokio::test]
async fn an_mcp_tool_call_without_mcp_servers_fails_only_that_call() {
    for version in [1, 2] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, server)) = mock_sequence_server(vec![
            // No MCP server is attached, so this tool was never offered.
            mock_named_tool_call_stream("mcp__absent__lookup"),
            mock_text_stream(),
        ])
        .await
        else {
            eprintln!("skipping missing MCP test: loopback networking is unavailable");
            return;
        };
        let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
        let stop_reason = if version == 1 {
            peer_raw_initialize(&mut peer).await;
            peer_prompt(&mut peer, temp.path(), "Look it up").await["stopReason"].clone()
        } else {
            v2_prompt_until_idle(&mut peer, temp.path(), "Look it up").await["params"]["update"]
                ["stopReason"]
                .clone()
        };
        assert_eq!(stop_reason, "end_turn", "v{version}");
        assert_eq!(
            final_tool_update(&peer, "call-mcp")["status"],
            "failed",
            "v{version}"
        );

        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2, "v{version}");
        let failed = tool_message(&requests[1], "call-mcp");
        assert!(
            failed.starts_with("MCP tool failed"),
            "v{version}: {failed}"
        );
    }
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
async fn session_cancel_ends_a_turn_waiting_on_an_mcp_tool_call() {
    let temp = tempfile::tempdir().unwrap();
    let Some((mcp_url, call_arrived, mcp_server)) =
        mock_stalled_http_mcp_server("tools/call").await
    else {
        eprintln!("skipping MCP cancellation test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) =
        mock_sequence_server(vec![mock_named_tool_call_stream("mcp__review__report")]).await
    else {
        eprintln!("skipping MCP cancellation test: loopback networking is unavailable");
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
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let prompt_id = peer.next_id;
    peer.next_id += 1;
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": prompt_id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "report"}]
        }
    }))
    .await;
    wait_for_stalled_mcp_call(&mut peer, call_arrived).await;

    // A request that needs the session's state is answered while the MCP
    // server still holds the call.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        peer.request(
            "session/set_mode",
            json!({"sessionId": session_id, "modeId": "agent"}),
        ),
    )
    .await
    .expect("session/set_mode should not wait for the MCP call");

    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message.get("id").and_then(Value::as_u64) == Some(prompt_id) {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("session/cancel should end the turn without waiting for the MCP call");
    assert_eq!(response["result"]["stopReason"], "cancelled");
    assert_eq!(final_tool_status(&peer, "call-mcp"), Some("failed"));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    mcp_server.abort();
}

#[tokio::test]
async fn v2_cancel_ends_work_waiting_on_an_mcp_tool_call() {
    let temp = tempfile::tempdir().unwrap();
    let Some((mcp_url, call_arrived, mcp_server)) =
        mock_stalled_http_mcp_server("tools/call").await
    else {
        eprintln!("skipping v2 MCP cancellation test: loopback networking is unavailable");
        return;
    };
    let Some((base_url, model_server)) =
        mock_sequence_server(vec![mock_named_tool_call_stream("mcp__review__report")]).await
    else {
        eprintln!("skipping v2 MCP cancellation test: loopback networking is unavailable");
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
            json!({
                "cwd": temp.path().to_string_lossy(),
                "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}],
                "additionalDirectories": []
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "report"}]
        }),
    )
    .await;
    wait_for_stalled_mcp_call(&mut peer, call_arrived).await;

    // A request that needs the session's state is answered while the MCP
    // server still holds the call.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        peer.request(
            "session/set_config_option",
            json!({
                "sessionId": session_id,
                "configId": "mode",
                "type": "id",
                "value": "agent"
            }),
        ),
    )
    .await
    .expect("session/set_config_option should not wait for the MCP call");

    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");
    assert_eq!(final_tool_status(&peer, "call-mcp"), Some("failed"));

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    mcp_server.abort();
}

#[tokio::test]
async fn session_cancel_ends_a_turn_waiting_on_an_mcp_tool_refresh() {
    let temp = tempfile::tempdir().unwrap();
    let Some((mcp_url, refresh_arrived, mcp_server)) =
        mock_stalled_http_mcp_server("tools/list").await
    else {
        eprintln!("skipping MCP refresh cancellation test: loopback networking is unavailable");
        return;
    };
    // The turn is cancelled before it asks the model anything.
    let Some((base_url, model_server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping MCP refresh cancellation test: loopback networking is unavailable");
        return;
    };
    let mut peer = spawn_peer(temp.path(), Some(&base_url)).await;
    peer.request(
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"session": {"notices": {}}}
        }),
    )
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
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let prompt_id = peer.next_id;
    peer.next_id += 1;
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": prompt_id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "report"}]
        }
    }))
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), refresh_arrived)
        .await
        .expect("the turn should refresh its MCP tools")
        .unwrap();

    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            if message.get("id").and_then(Value::as_u64) == Some(prompt_id) {
                return message;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("session/cancel should end the turn without waiting for tools/list");
    assert_eq!(response["result"]["stopReason"], "cancelled");
    assert!(!reported_refresh_failure(&peer), "{:?}", peer.updates);

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    mcp_server.abort();
}

#[tokio::test]
async fn v2_cancel_ends_work_waiting_on_an_mcp_tool_refresh() {
    let temp = tempfile::tempdir().unwrap();
    let Some((mcp_url, refresh_arrived, mcp_server)) =
        mock_stalled_http_mcp_server("tools/list").await
    else {
        eprintln!("skipping v2 MCP refresh cancellation test: loopback networking is unavailable");
        return;
    };
    // The work is cancelled before it asks the model anything.
    let Some((base_url, model_server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping v2 MCP refresh cancellation test: loopback networking is unavailable");
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
            json!({
                "cwd": temp.path().to_string_lossy(),
                "mcpServers": [{"type":"http", "name":"review", "url":mcp_url, "headers":[]}],
                "additionalDirectories": []
            }),
        )
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    peer.request(
        "session/prompt",
        json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "report"}]
        }),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), refresh_arrived)
        .await
        .expect("the work should refresh its MCP tools")
        .unwrap();

    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id}
    }))
    .await;
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");
    assert!(!reported_refresh_failure(&peer), "{:?}", peer.updates);

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    model_server.abort();
    mcp_server.abort();
}

/// Whether the agent sent a notice that refreshing MCP tools failed.
fn reported_refresh_failure(peer: &Peer) -> bool {
    peer.updates.iter().any(|update| {
        update["params"]["update"]["sessionUpdate"] == "notice"
            && update["params"]["update"]["title"] == "MCP tool refresh failed"
    })
}

/// Reads updates until the model's MCP tool call is running, then waits for it
/// to reach `mock_stalled_http_mcp_server`.
async fn wait_for_stalled_mcp_call(
    peer: &mut Peer,
    call_arrived: tokio::sync::oneshot::Receiver<()>,
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let message = peer.next_message().await;
            let running = message["params"]["update"]["sessionUpdate"] == "tool_call_update"
                && message["params"]["update"]["toolCallId"] == "call-mcp"
                && message["params"]["update"]["status"] == "in_progress";
            peer.updates.push(message);
            if running {
                break;
            }
        }
        call_arrived.await.unwrap();
    })
    .await
    .expect("the model's MCP tool call should reach the MCP server");
}

/// The status in the last update for the tool call that has one.
fn final_tool_status<'a>(peer: &'a Peer, tool_call_id: &str) -> Option<&'a str> {
    peer.updates
        .iter()
        .rev()
        .map(|update| &update["params"]["update"])
        .filter(|update| update["toolCallId"] == tool_call_id)
        .find_map(|update| update["status"].as_str())
}

#[tokio::test]
async fn cancel_request_ends_an_in_flight_prompt_with_request_cancelled() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_slow_deepseek_server().await else {
        eprintln!("skipping request cancellation test: loopback networking is unavailable");
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
    let cancel_request = json!({
        "jsonrpc": "2.0",
        "method": "$/cancel_request",
        "params": {"requestId": prompt_id}
    });

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
                peer.write(cancel_request.clone()).await;
                cancellation_sent = true;
            }
            peer.updates.push(message);
        }
    })
    .await
    .expect("request cancellation should end the turn promptly");
    assert!(cancellation_sent);
    assert!(response.get("result").is_none(), "{response}");
    assert_eq!(response["error"]["code"], -32800, "{response}");

    // Cancelling the answered request again changes nothing, and the session
    // takes the next prompt because the turn has ended. `request` fails on
    // any other response to the cancelled prompt.
    peer.write(cancel_request).await;
    let status = peer
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "/status"}]}),
        )
        .await;
    assert_eq!(status["stopReason"], "end_turn");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    server.abort();
}

#[tokio::test]
async fn cancel_request_withdraws_a_pending_permission_and_fails_its_tool_call() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, server)) = mock_sequence_server(vec![mock_tool_call_stream()]).await else {
        eprintln!("skipping request cancellation test: loopback networking is unavailable");
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
    let prompt_id = peer.next_id;
    peer.next_id += 1;
    peer.write(json!({
        "jsonrpc": "2.0",
        "id": prompt_id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "write the sample file"}]
        }
    }))
    .await;

    let mut permission_id = None;
    let mut permission_cancelled = false;
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            // Leave the permission request unanswered until the agent
            // withdraws it.
            let message = peer.read_message().await;
            match message.get("method").and_then(Value::as_str) {
                Some("session/request_permission") => {
                    permission_id = Some(message["id"].clone());
                    peer.write(json!({
                        "jsonrpc": "2.0",
                        "method": "$/cancel_request",
                        "params": {"requestId": prompt_id}
                    }))
                    .await;
                }
                Some("$/cancel_request") => {
                    assert_eq!(
                        Some(&message["params"]["requestId"]),
                        permission_id.as_ref()
                    );
                    permission_cancelled = true;
                    // Answer as a client that supports cancellation must.
                    peer.write(json!({
                        "jsonrpc": "2.0",
                        "id": message["params"]["requestId"],
                        "error": {"code": -32800, "message": "Request cancelled"}
                    }))
                    .await;
                }
                Some("session/update") => peer.updates.push(message),
                _ if message.get("id").and_then(Value::as_u64) == Some(prompt_id) => {
                    return message;
                }
                _ => panic!("unexpected message: {message}"),
            }
        }
    })
    .await
    .expect("request cancellation should end the turn promptly");
    assert!(permission_id.is_some());
    assert!(permission_cancelled);
    assert_eq!(response["error"]["code"], -32800, "{response}");
    let finished = peer
        .updates
        .iter()
        .map(|update| &update["params"]["update"])
        .rfind(|update| update["toolCallId"] == "call-1")
        .unwrap();
    assert_eq!(finished["status"], "failed");
    assert!(!temp.path().join("sample.txt").exists());

    let status = peer
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "/status"}]}),
        )
        .await;
    assert_eq!(status["stopReason"], "end_turn");

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 1);

    // The turn itself saved the failed outcome. Reading the record from disk
    // matters: loading a session also marks unanswered tool calls failed, so
    // replay would look the same if nothing had been saved.
    let saved: Value = serde_json::from_slice(
        &std::fs::read(
            temp.path()
                .join("sessions-data/sessions")
                .join(format!("{session_id}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        saved["tool_outcomes"]["call-1"]["status"], "failed",
        "{saved}"
    );
}

#[tokio::test]
async fn session_cancel_with_cancel_request_still_ends_the_prompt_cancelled() {
    for cancel_request_first in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let Some((base_url, server)) = mock_slow_deepseek_server().await else {
            eprintln!("skipping cancellation test: loopback networking is unavailable");
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
        let session_cancel = json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id}
        });
        let cancel_request = json!({
            "jsonrpc": "2.0",
            "method": "$/cancel_request",
            "params": {"requestId": prompt_id}
        });
        let mut notifications = Some(if cancel_request_first {
            [cancel_request, session_cancel]
        } else {
            [session_cancel, cancel_request]
        });

        let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let message = peer.next_message().await;
                if message.get("id").and_then(Value::as_u64) == Some(prompt_id) {
                    return message;
                }
                if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
                    && let Some(notifications) = notifications.take()
                {
                    // One write, so the agent has read session/cancel before
                    // the turn can answer.
                    peer.write_together(&notifications).await;
                }
                peer.updates.push(message);
            }
        })
        .await
        .expect("cancellation should complete promptly");
        assert!(notifications.is_none());
        assert_eq!(
            response["result"]["stopReason"], "cancelled",
            "cancel_request_first={cancel_request_first}: {response}"
        );

        // `request` fails on any other response to the cancelled prompt.
        let status = peer
            .request(
                "session/prompt",
                json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "/status"}]}),
            )
            .await;
        assert_eq!(status["stopReason"], "end_turn");

        drop(peer.stdin);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
        server.abort();
    }
}

#[tokio::test]
async fn v2_cancel_request_after_acceptance_leaves_the_work_running() {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, release, server)) = mock_gated_deepseek_server().await else {
        eprintln!("skipping v2 request cancellation test: loopback networking is unavailable");
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
    let prompt_id = peer.next_id;
    let accepted = peer
        .request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "keep working"}]
            }),
        )
        .await;
    assert!(accepted["messageId"].is_string());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !peer
            .updates
            .iter()
            .any(|update| update["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        {
            let message = peer.next_message().await;
            peer.updates.push(message);
        }
    })
    .await
    .expect("the model stream should start");

    // ACP v2 answered the prompt at insertion, so cancelling that request
    // cannot stop the work it started; only session/cancel does. The list
    // round trip makes sure the notification was handled before the model
    // is allowed to finish.
    peer.write(json!({
        "jsonrpc": "2.0",
        "method": "$/cancel_request",
        "params": {"requestId": prompt_id}
    }))
    .await;
    peer.request("session/list", json!({"cwd": cwd})).await;
    release.send(()).unwrap();
    let idle = next_v2_idle(&mut peer).await;
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert!(
        peer.updates
            .iter()
            .all(|message| message.get("id").and_then(Value::as_u64) != Some(prompt_id))
    );

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    server.await.unwrap();
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

/// Streams one chunk of an answer and finishes it only after `release` fires.
async fn mock_gated_deepseek_server() -> Option<(
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_json_body(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let first = format!(
            "data: {}\n\n",
            json!({"choices": [{"delta": {"content": "working"}}]})
        );
        socket
            .write_all(format!("{:x}\r\n{first}\r\n", first.len()).as_bytes())
            .await
            .unwrap();
        released.await.unwrap();
        let rest = encode_sse(&[
            json!({"choices": [{"delta": {"content": " done"}, "finish_reason": "stop"}]}),
            json!({"choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}}),
        ]);
        socket
            .write_all(format!("{:x}\r\n{rest}\r\n0\r\n\r\n", rest.len()).as_bytes())
            .await
            .unwrap();
        socket.shutdown().await.unwrap();
    });
    Some((format!("http://{address}"), release, task))
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

/// A mock model that answers every request with text and reports each
/// request's `Authorization` header, or `None` when it had none, before
/// answering it.
async fn mock_authorization_recording_server()
-> Option<(String, tokio::sync::mpsc::UnboundedReceiver<Option<String>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (headers, _) = read_http_request(&mut socket).await;
            let authorization = headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim_start().to_string())
            });
            let _ = sender.send(authorization);
            let response = mock_text_stream();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                response.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    Some((format!("http://{address}"), receiver))
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

/// Like [`mock_sequence_server`], but a `None` response streams the text
/// "working" and then holds the stream open until the agent hangs up, so the
/// test can cancel the turn while the model is still answering.
async fn mock_stalling_sequence_server(
    responses: Vec<Option<String>>,
) -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            requests.push(read_http_json_body(&mut socket).await);
            let Some(response) = response else {
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let payload = format!(
                    "data: {}\n\n",
                    json!({"choices": [{"delta": {"content": "working"}}]})
                );
                socket
                    .write_all(format!("{:x}\r\n{payload}\r\n", payload.len()).as_bytes())
                    .await
                    .unwrap();
                let mut rest = [0u8; 1024];
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while socket.read(&mut rest).await.is_ok_and(|read| read > 0) {}
                })
                .await
                .expect("the agent should hang up on a cancelled stream");
                continue;
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

/// Holds the reply to the first `tools/call`: `started` fires when the call
/// arrives, and the reply waits for `release`.
struct HeldMcpCall {
    started: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

async fn mock_http_mcp_server(
    request_count: usize,
    mut held_call: Option<HeldMcpCall>,
) -> Option<(String, tokio::task::JoinHandle<Vec<Value>>)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..request_count {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_json_body(&mut socket).await;
            if request["method"] == "tools/call"
                && let Some(held) = held_call.take()
            {
                held.started.send(()).unwrap();
                held.release.await.unwrap();
            }
            answer_mock_mcp_request(&mut socket, &request).await;
            requests.push(request);
        }
        requests
    });
    Some((format!("http://{address}"), task))
}

/// How long `mock_stalled_http_mcp_server` holds the request it stalls: far
/// longer than a test waits for a cancelled turn to end.
const STALLED_MCP_REQUEST: std::time::Duration = std::time::Duration::from_secs(30);

/// Serves MCP like `mock_http_mcp_server`, but answers the first `method`
/// request after the connection handshake only after `STALLED_MCP_REQUEST`.
/// The receiver fires once that request has arrived.
async fn mock_stalled_http_mcp_server(
    method: &'static str,
) -> Option<(
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<()>,
)> {
    let listener = loopback_listener().await?;
    let address = listener.local_addr().unwrap();
    let (arrived, stalled) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut arrived = Some(arrived);
        // initialize, notifications/initialized, and tools/list.
        let mut handshake = 3;
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_json_body(&mut socket).await;
            if handshake > 0 {
                handshake -= 1;
            } else if request["method"] == method
                && let Some(arrived) = arrived.take()
            {
                let _ = arrived.send(());
                tokio::time::sleep(STALLED_MCP_REQUEST).await;
            }
            answer_mock_mcp_request(&mut socket, &request).await;
        }
    });
    Some((format!("http://{address}"), stalled, task))
}

async fn answer_mock_mcp_request(socket: &mut tokio::net::TcpStream, request: &Value) {
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
    if request.get("id").is_none() {
        socket
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
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

/// Waits for the mock MCP server to serve every request it expects, so a
/// miscount fails the test instead of hanging it.
async fn served_mcp_requests(server: tokio::task::JoinHandle<Vec<Value>>) -> Vec<Value> {
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .expect("mock MCP server saw fewer requests than expected")
        .unwrap()
}

async fn loopback_listener() -> Option<TcpListener> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
        Err(error) => panic!("failed to bind loopback mock server: {error}"),
    }
}

async fn read_http_json_body(socket: &mut tokio::net::TcpStream) -> Value {
    read_http_request(socket).await.1
}

/// The request line and headers, and the JSON body, of one HTTP request.
async fn read_http_request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
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
    let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
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
    let body = serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
    (headers, body)
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

async fn peer_new_session(peer: &mut Peer, cwd: &str) -> String {
    let session = peer
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
        )
        .await;
    session["sessionId"].as_str().unwrap().to_string()
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

async fn next_v2_idle(peer: &mut Peer) -> Value {
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
    .expect("v2 prompt should settle to idle")
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
