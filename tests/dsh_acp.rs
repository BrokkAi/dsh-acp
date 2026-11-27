//! End-to-end ACP coverage against the pinned official DSH runtime.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStdin, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

struct Peer {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    updates: Vec<Value>,
}

impl Peer {
    async fn spawn(home: &Path, data: &Path, base_url: &str) -> Self {
        let dsh_js = real_dsh_js();
        let mut child = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
            .arg("--dsh-js")
            .arg(dsh_js)
            .arg("--node")
            .arg(std::env::var("DSH_ACP_TEST_NODE").unwrap_or_else(|_| "node".to_string()))
            .arg("--dsh-home")
            .arg(home)
            .arg("--data-dir")
            .arg(data)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("DEEPSEEK_API_KEY", "dsh-acp-test")
            .env("DEEPSEEK_BASE_URL", base_url)
            .env_remove("DSH_ACP_DSH_COMMAND")
            .env_remove("DSH_ACP_DSH_JS")
            .env_remove("RUST_LOG")
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
            updates: Vec::new(),
        }
    }

    async fn initialize(&mut self, version: u64) {
        let params = if version == 2 {
            json!({
                "protocolVersion": 2,
                "info": {"name": "dsh-acp-test", "version": "1"},
                "capabilities": {}
            })
        } else {
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {}
            })
        };
        let response = self.request(1, "initialize", params).await;
        assert_eq!(response["protocolVersion"], version);
    }

    async fn new_session(&mut self, id: u64, cwd: &Path) -> String {
        let response = self
            .request(
                id,
                "session/new",
                json!({"cwd": cwd, "mcpServers": [], "additionalDirectories": []}),
            )
            .await;
        response["sessionId"].as_str().unwrap().to_string()
    }

    async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.start_request(id, method, params).await;
        self.await_response(id).await
    }

    async fn start_request(&mut self, id: u64, method: &str, params: Value) {
        self.write(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await;
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.write(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await;
    }

    async fn await_response(&mut self, id: u64) -> Value {
        timeout(TEST_TIMEOUT, async {
            loop {
                let message = self.next().await;
                if message.get("method").is_some() {
                    self.handle_server_message(message).await;
                    continue;
                }
                if message.get("id") == Some(&json!(id)) {
                    assert!(message.get("error").is_none(), "ACP error: {message}");
                    return message["result"].clone();
                }
            }
        })
        .await
        .expect("timed out waiting for ACP response")
    }

    async fn next(&mut self) -> Value {
        let mut line = String::new();
        let read = timeout(TEST_TIMEOUT, self.stdout.read_line(&mut line))
            .await
            .expect("timed out waiting for ACP output")
            .unwrap();
        assert_ne!(read, 0, "dsh-acp stdout closed");
        serde_json::from_str(&line).unwrap()
    }

    async fn handle_server_message(&mut self, message: Value) {
        match message.get("method").and_then(Value::as_str) {
            Some("session/update") => {
                self.updates.push(message["params"]["update"].clone());
            }
            Some("session/request_permission") => {
                let option = message["params"]["options"][0]["optionId"].clone();
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {"outcome": {"outcome": "selected", "optionId": option}}
                }))
                .await;
            }
            Some(method) => panic!("unexpected server request: {method}: {message}"),
            None => {}
        }
    }

    async fn wait_for_idle(&mut self) -> Value {
        timeout(TEST_TIMEOUT, async {
            loop {
                let message = self.next().await;
                if message.get("method") == Some(&json!("session/update")) {
                    let update = message["params"]["update"].clone();
                    if update["sessionUpdate"] == "state_update" && update["state"] == "idle" {
                        return update;
                    }
                    self.updates.push(update);
                } else {
                    self.handle_server_message(message).await;
                }
            }
        })
        .await
        .expect("ACP v2 prompt did not become idle")
    }

    fn streamed_text(&self) -> String {
        self.updates
            .iter()
            .filter(|update| update["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|update| update["content"]["text"].as_str())
            .collect()
    }

    async fn write(&mut self, value: Value) {
        self.stdin
            .write_all(format!("{}\n", serde_json::to_string(&value).unwrap()).as_bytes())
            .await
            .unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn shutdown(self) {
        let mut child = self.child;
        drop(self.stdin);
        let status = timeout(TEST_TIMEOUT, child.wait())
            .await
            .expect("bridge did not exit")
            .unwrap();
        assert!(status.success(), "bridge exited with {status}");
    }
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v1_streams_from_real_dsh_and_reopens_the_persisted_session() {
    let server = MessagesServer::text("Hello from DSH", 2).await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(1).await;
    let session_id = peer.new_session(2, workspace.path()).await;
    let result = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Say hello"}]}),
        )
        .await;
    assert_eq!(result["stopReason"], "end_turn");
    assert_eq!(peer.streamed_text(), "Hello from DSH");
    let forked = peer
        .request(
            4,
            "session/fork",
            json!({
                "sessionId": session_id,
                "cwd": workspace.path(),
                "mcpServers": [],
                "additionalDirectories": []
            }),
        )
        .await;
    let fork_id = forked["sessionId"].as_str().unwrap().to_string();
    peer.request(
        5,
        "session/prompt",
        json!({"sessionId": fork_id, "prompt": [{"type": "text", "text": "Continue"}]}),
    )
    .await;
    assert_eq!(peer.streamed_text(), "Hello from DSHHello from DSH");
    peer.request(6, "session/close", json!({"sessionId": fork_id}))
        .await;
    assert_eq!(server.requests().await, 2);

    peer.request(
        7,
        "session/set_mode",
        json!({"sessionId": session_id, "modeId": "plan"}),
    )
    .await;
    let configured = peer
        .request(
            8,
            "session/set_config_option",
            json!({
                "sessionId": session_id,
                "configId": "model",
                "value": "deepseek-v4-pro"
            }),
        )
        .await;
    assert!(
        configured["configOptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|option| option["id"] == "model" && option["currentValue"] == "deepseek-v4-pro")
    );
    peer.request(
        9,
        "session/set_title",
        json!({"sessionId": session_id, "title": "DSH bridge test"}),
    )
    .await;
    peer.request(10, "session/close", json!({"sessionId": session_id}))
        .await;
    let listed = peer.request(11, "session/list", json!({})).await;
    assert!(
        listed["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| {
                session["sessionId"] == session_id && session["title"] == "DSH bridge test"
            })
    );
    peer.request(
        12,
        "session/resume",
        json!({"sessionId": session_id, "cwd": workspace.path(), "mcpServers": []}),
    )
    .await;
    peer.request(13, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v1_permission_request_controls_a_real_dsh_write() {
    let server = MessagesServer::permission_write().await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(1).await;
    let session_id = peer.new_session(2, workspace.path()).await;
    let result = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Write approved.txt"}]}),
        )
        .await;
    assert_eq!(result["stopReason"], "end_turn");
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("approved.txt")).unwrap(),
        "approved write"
    );
    assert_eq!(server.requests().await, 2);
    peer.request(4, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v1_cancel_stops_the_real_dsh_model_stream() {
    let server = MessagesServer::hang().await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(1).await;
    let session_id = peer.new_session(2, workspace.path()).await;
    peer.start_request(
        3,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Wait forever"}]}),
    )
    .await;
    server.wait_until_requested().await;
    peer.notify("session/cancel", json!({"sessionId": session_id}))
        .await;
    let result = peer.await_response(3).await;
    assert_eq!(result["stopReason"], "cancelled");
    peer.request(4, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v2_keeps_asynchronous_prompt_acceptance_and_dsh_streaming() {
    let server = MessagesServer::text("V2 from DSH", 1).await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(2).await;
    let session_id = peer.new_session(2, workspace.path()).await;
    let accepted = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Say hello"}]}),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    let idle = peer.wait_for_idle().await;
    assert_eq!(idle["stopReason"], "end_turn");
    assert_eq!(peer.streamed_text(), "V2 from DSH");
    server.requests().await;
    peer.request(4, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v2_cancel_settles_the_dsh_turn_to_idle() {
    let server = MessagesServer::hang().await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(2).await;
    let session_id = peer.new_session(2, workspace.path()).await;
    let accepted = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Wait forever"}]}),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    server.wait_until_requested().await;
    peer.notify("session/cancel", json!({"sessionId": session_id}))
        .await;
    let idle = peer.wait_for_idle().await;
    assert_eq!(idle["stopReason"], "cancelled");
    peer.request(4, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v1_native_dsh_usage_sums_steps_and_reports_turn_deltas() {
    let first = json!({
        "inputTokens": 10,
        "outputTokens": 10,
        "cacheReadTokens": 7,
        "cacheWriteTokens": 3
    });
    let second = json!({
        "inputTokens": 4,
        "outputTokens": 2,
        "cacheReadTokens": 5,
        "cacheWriteTokens": 1
    });
    let third = json!({
        "inputTokens": 6,
        "outputTokens": 4,
        "cacheReadTokens": 2,
        "cacheWriteTokens": 1
    });
    let server = MessagesServer::scripted(
        vec![
            tool_write_response_with_usage(&first),
            text_response_with_usage("write completed", &second),
            text_response_with_usage("second answer", &third),
        ],
        3,
    )
    .await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(1).await;
    let session_id = peer.new_session(2, workspace.path()).await;

    let result = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Write approved.txt"}]}),
        )
        .await;
    assert_eq!(result["stopReason"], "end_turn");
    assert_turn_usage(&result["usage"], 30, 12, 12, 4);
    assert_eq!(
        usage_updates(&peer),
        vec![30, 12],
        "context_used follows each model call, not the cumulative turn total"
    );
    assert_eq!(peer.streamed_text(), "write completed");

    let result = peer
        .request(
            4,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Continue"}]}),
        )
        .await;
    assert_eq!(result["stopReason"], "end_turn");
    assert_turn_usage(&result["usage"], 9, 4, 2, 1);
    assert_eq!(
        usage_updates(&peer),
        vec![30, 12, 13],
        "the second turn reports its own last call"
    );
    assert_eq!(peer.streamed_text(), "write completedsecond answer");
    assert_eq!(server.requests().await, 3);
    peer.request(5, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn v2_native_dsh_usage_sums_steps_and_reports_turn_deltas() {
    let first = json!({
        "inputTokens": 10,
        "outputTokens": 10,
        "cacheReadTokens": 7,
        "cacheWriteTokens": 3
    });
    let second = json!({
        "inputTokens": 4,
        "outputTokens": 2,
        "cacheReadTokens": 5,
        "cacheWriteTokens": 1
    });
    let third = json!({
        "inputTokens": 6,
        "outputTokens": 4,
        "cacheReadTokens": 2,
        "cacheWriteTokens": 1
    });
    let server = MessagesServer::scripted(
        vec![
            tool_write_response_with_usage(&first),
            text_response_with_usage("write completed", &second),
            text_response_with_usage("second answer", &third),
        ],
        3,
    )
    .await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(2).await;
    let session_id = peer.new_session(2, workspace.path()).await;

    let accepted = peer
        .request(
            3,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Write approved.txt"}]}),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    let idle = peer.wait_for_idle().await;
    assert_eq!(idle["stopReason"], "end_turn");
    assert_turn_usage(&idle["usage"], 30, 12, 12, 4);
    assert_eq!(
        usage_updates(&peer),
        vec![30, 12],
        "context_used follows each model call, not the cumulative turn total"
    );
    assert_eq!(peer.streamed_text(), "write completed");

    let accepted = peer
        .request(
            4,
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "Continue"}]}),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    let idle = peer.wait_for_idle().await;
    assert_eq!(idle["stopReason"], "end_turn");
    assert_turn_usage(&idle["usage"], 9, 4, 2, 1);
    assert_eq!(
        usage_updates(&peer),
        vec![30, 12, 13],
        "the second turn reports its own last call"
    );
    assert_eq!(peer.streamed_text(), "write completedsecond answer");
    assert_eq!(server.requests().await, 3);
    peer.request(5, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn session_new_mounts_an_acp_mcp_server_in_dsh() {
    let server = MessagesServer::scripted(vec![], 0).await;
    let home = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let fixture = workspace.path().join("mcp-server.mjs");
    std::fs::write(
        &fixture,
        r#"
import readline from 'node:readline';
const rl = readline.createInterface({ input: process.stdin });
for await (const line of rl) {
  const request = JSON.parse(line);
  if (request.id === undefined) continue;
  let result;
  if (request.method === 'initialize') {
    result = {
      protocolVersion: '2025-06-18',
      capabilities: { tools: {} },
      serverInfo: { name: 'fixture', version: '1.0.0' }
    };
  } else if (request.method === 'tools/list') {
    result = {
      tools: [{
        name: 'echo',
        description: 'Echo one value',
        inputSchema: {
          type: 'object',
          properties: { value: { type: 'string' } },
          required: ['value']
        }
      }]
    };
  } else {
    result = {};
  }
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result }) + '\n');
}
"#,
    )
    .unwrap();
    let mut peer = Peer::spawn(home.path(), data.path(), &server.base_url).await;
    peer.initialize(1).await;
    let created = peer
        .request(
            2,
            "session/new",
            json!({
                "cwd": workspace.path(),
                "mcpServers": [{
                    "name": "fixture",
                    "command": executable("node"),
                    "args": [fixture],
                    "env": []
                }],
                "additionalDirectories": []
            }),
        )
        .await;
    let session_id = created["sessionId"].as_str().unwrap().to_string();
    peer.request(3, "session/close", json!({"sessionId": session_id}))
        .await;
    peer.shutdown().await;
}

fn real_dsh_js() -> PathBuf {
    std::env::var_os("DSH_ACP_REAL_DSH_JS")
        .map(PathBuf::from)
        .expect("DSH_ACP_REAL_DSH_JS must name @deepseek-ai/dsh/lib/bin.js")
}

fn executable(name: &str) -> PathBuf {
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} is not on PATH"))
}

struct MessagesServer {
    base_url: String,
    requests: JoinHandle<usize>,
    observed: tokio::sync::watch::Receiver<usize>,
}

impl MessagesServer {
    async fn text(text: &'static str, expected: usize) -> Self {
        let responses = vec![text_response(text)];
        Self::scripted(responses, expected).await
    }

    async fn permission_write() -> Self {
        Self::scripted(
            vec![tool_write_response(), text_response("write completed")],
            2,
        )
        .await
    }

    async fn scripted(responses: Vec<Vec<Value>>, expected: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/anthropic", listener.local_addr().unwrap());
        let (observed_tx, observed) = tokio::sync::watch::channel(0);
        let requests = tokio::spawn(async move {
            for index in 0..expected {
                let (mut socket, _) = listener.accept().await.unwrap();
                let _ = read_http_json(&mut socket).await;
                observed_tx.send_replace(index + 1);
                let response = &responses[index.min(responses.len() - 1)];
                write_sse(&mut socket, response).await;
            }
            expected
        });
        Self {
            base_url,
            requests,
            observed,
        }
    }

    async fn hang() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/anthropic", listener.local_addr().unwrap());
        let (observed_tx, observed) = tokio::sync::watch::channel(0);
        let requests = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_json(&mut socket).await;
            observed_tx.send_replace(1);
            let body = sse(&[json!({
                "type": "message_start",
                "message": {
                    "id": "msg_hang",
                    "model": "deepseek-v4-flash",
                    "usage": {"input_tokens": 3, "output_tokens": 0}
                }
            })]);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            socket
                .write_all(format!("{:x}\r\n{body}\r\n", body.len()).as_bytes())
                .await
                .unwrap();
            socket.flush().await.unwrap();
            sleep(Duration::from_secs(30)).await;
            1
        });
        Self {
            base_url,
            requests,
            observed,
        }
    }

    async fn wait_until_requested(&self) {
        let mut observed = self.observed.clone();
        timeout(TEST_TIMEOUT, async {
            while *observed.borrow() == 0 {
                observed.changed().await.unwrap();
            }
        })
        .await
        .expect("mock model did not receive the prompt");
    }

    async fn requests(self) -> usize {
        self.requests.await.unwrap()
    }
}

fn text_response(text: &str) -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_text","model":"deepseek-v4-flash","usage":{"input_tokens":3,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ]
}

fn tool_write_response() -> Vec<Value> {
    tool_write_response_with_usage(&json!({
        "inputTokens": 10,
        "outputTokens": 10,
        "cacheReadTokens": 0,
        "cacheWriteTokens": 0
    }))
}

/// The fake Messages API usage fields DSH maps onto its disjoint counters.
fn wire_usage(usage: &Value) -> Value {
    json!({
        "input_tokens": usage["inputTokens"],
        "output_tokens": 0,
        "cache_read_input_tokens": usage["cacheReadTokens"],
        "cache_creation_input_tokens": usage["cacheWriteTokens"],
    })
}

fn text_response_with_usage(text: &str, usage: &Value) -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_text","model":"deepseek-v4-flash","usage":wire_usage(usage)}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":usage["outputTokens"]}}),
        json!({"type":"message_stop"}),
    ]
}

fn tool_write_response_with_usage(usage: &Value) -> Vec<Value> {
    let arguments = json!({
        "file_path": "approved.txt",
        "content": "approved write",
        "sandbox_permissions": "danger-full-access",
        "justification": "The user asked for this exact file write."
    });
    vec![
        json!({"type":"message_start","message":{"id":"msg_tool","model":"deepseek-v4-flash","usage":wire_usage(usage)}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"write","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":arguments.to_string()}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":usage["outputTokens"]}}),
        json!({"type":"message_stop"}),
    ]
}

fn usage_updates(peer: &Peer) -> Vec<u64> {
    peer.updates
        .iter()
        .filter(|update| update["sessionUpdate"] == "usage_update")
        .filter_map(|update| update["used"].as_u64())
        .collect()
}

/// A turn-scoped usage report whose input includes cache tokens, as Mjolnir
/// prices it, with the cache buckets also declared separately.
fn assert_turn_usage(usage: &Value, input: u64, output: u64, read: u64, write: u64) {
    assert_eq!(usage["inputTokens"].as_u64(), Some(input), "usage: {usage}");
    assert_eq!(
        usage["outputTokens"].as_u64(),
        Some(output),
        "usage: {usage}"
    );
    assert_eq!(
        usage["totalTokens"].as_u64(),
        Some(input + output),
        "usage: {usage}"
    );
    assert_eq!(
        usage["cachedReadTokens"].as_u64(),
        Some(read),
        "usage: {usage}"
    );
    assert_eq!(
        usage["cachedWriteTokens"].as_u64(),
        Some(write),
        "usage: {usage}"
    );
    assert_eq!(
        usage["inputTokens"].as_u64().unwrap() + usage["outputTokens"].as_u64().unwrap(),
        usage["totalTokens"].as_u64().unwrap(),
        "input + output must equal total: {usage}"
    );
    assert_eq!(
        usage["_meta"]["mjolnir.dev/usage-scope"].as_str(),
        Some("turn"),
        "usage: {usage}"
    );
    assert!(
        usage.get("thoughtTokens").is_none(),
        "DeepSeek reports no separate reasoning count: {usage}"
    );
}

async fn read_http_json(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
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
        let mut chunk = [0u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap()
}

async fn write_sse(socket: &mut TcpStream, events: &[Value]) {
    let body = sse(events);
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    socket.write_all(body.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
}

fn sse(events: &[Value]) -> String {
    events
        .iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {}\n\n",
                event["type"].as_str().unwrap(),
                serde_json::to_string(event).unwrap()
            )
        })
        .collect()
}
