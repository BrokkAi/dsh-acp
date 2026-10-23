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
}

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
    let mut command = Command::new(env!("CARGO_BIN_EXE_dsh-acp"));
    command
        .arg("--data-dir")
        .arg(data_dir.join("sessions-data"))
        .arg("--disable-web")
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

    drop(peer.stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), peer.child.wait()).await;
    assert_eq!(server.await.unwrap().len(), 2);
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
