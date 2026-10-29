//! ACP over streamable HTTP and WebSocket (`--listen`), driven through the
//! real binary.

use std::{
    net::SocketAddr,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::{Channel, Client, ConnectTo, TransportFrame};
use agent_client_protocol_http::HttpClient;
use futures_util::StreamExt;
use reqwest::{StatusCode, header};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const BUSY: &str = "another ACP connection is active; dsh-acp serves one connection at a time";
const ENV_TOKEN: &str = "test-token-0123456789abcdef";

/// A `dsh-acp --listen` process, killed on drop.
struct Server {
    _child: Child,
    address: SocketAddr,
    token: String,
    announcement: String,
    stderr: Arc<Mutex<String>>,
}

impl Server {
    async fn start(data_dir: &Path, base_url: Option<&str>, extra: &[&str]) -> Self {
        Self::start_with(data_dir, base_url, extra, &[]).await
    }

    async fn start_with(
        data_dir: &Path,
        base_url: Option<&str>,
        extra: &[&str],
        env: &[(&str, &str)],
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dsh-acp"));
        if !extra.contains(&"--listen") {
            command.arg("--listen").arg("127.0.0.1:0");
        }
        command
            .arg("--data-dir")
            .arg(data_dir.join("sessions-data"))
            .arg("--disable-web")
            .args(extra)
            .env_remove("DSH_ACP_LISTEN_TOKEN")
            .env("DEEPSEEK_API_KEY", "test-key")
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(base_url) = base_url {
            command.arg("--base-url").arg(base_url);
        }
        let mut child = command.spawn().unwrap();
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let announcement = tokio::time::timeout(TIMEOUT, async {
            loop {
                let line = lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("server exited before announcing its endpoint");
                if line.starts_with("dsh-acp: serving ACP at ") {
                    return line;
                }
            }
        })
        .await
        .expect("server did not announce its endpoint");
        let stderr = Arc::new(Mutex::new(String::new()));
        tokio::spawn({
            let stderr = stderr.clone();
            async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut stderr = stderr.lock().unwrap();
                    stderr.push_str(&line);
                    stderr.push('\n');
                }
            }
        });
        let endpoint = announcement
            .strip_prefix("dsh-acp: serving ACP at http://")
            .and_then(|rest| rest.split_once("/acp and ws://"))
            .map(|(address, _)| address)
            .expect("announcement names the HTTP endpoint");
        let address = endpoint.parse().unwrap();
        assert!(
            announcement.contains(&format!("ws://{address}/acp")),
            "{announcement}"
        );
        let token = announcement
            .split_once("Authorization: Bearer ")
            .map(|(_, token)| token.trim_end_matches('"').to_string())
            .expect("announcement says how to authenticate");
        Self {
            _child: child,
            address,
            token,
            announcement,
            stderr,
        }
    }

    fn http_url(&self) -> String {
        format!("http://{}/acp", self.address)
    }

    fn ws_url(&self) -> String {
        format!("ws://{}/acp", self.address)
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.token)
    }

    fn client(&self) -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    fn post(&self, body: &Value) -> reqwest::RequestBuilder {
        self.client()
            .post(self.http_url())
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
    }

    /// POST `initialize` with the token and return the connection ID.
    async fn initialize(&self) -> String {
        let response = self
            .post(&initialize_request(1))
            .header(header::AUTHORIZATION, self.bearer())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let connection_id = response.headers()["acp-connection-id"]
            .to_str()
            .unwrap()
            .to_string();
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["result"]["protocolVersion"], 1, "{body}");
        connection_id
    }

    /// Open the connection-scoped event stream.
    async fn events(&self, connection_id: &str) -> EventStream {
        let response = self
            .client()
            .get(self.http_url())
            .header(header::ACCEPT, "text/event-stream")
            .header(header::AUTHORIZATION, self.bearer())
            .header("acp-connection-id", connection_id)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        EventStream {
            body: response
                .bytes_stream()
                .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
                .boxed(),
            buffer: String::new(),
        }
    }

    async fn delete(&self, connection_id: &str) {
        let response = self
            .client()
            .delete(self.http_url())
            .header(header::AUTHORIZATION, self.bearer())
            .header("acp-connection-id", connection_id)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    /// A WebSocket upgrade request, without completing the handshake.
    fn upgrade(&self) -> reqwest::RequestBuilder {
        self.client()
            .get(self.http_url())
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
    }

    fn stderr(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }
}

/// Server-sent events from one GET stream.
struct EventStream {
    body: futures_util::stream::BoxStream<'static, reqwest::Result<Vec<u8>>>,
    buffer: String,
}

impl EventStream {
    async fn next_message(&mut self) -> Value {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(end) = self.buffer.find("\n\n") {
                    let event = self.buffer[..end].to_string();
                    self.buffer.drain(..end + 2);
                    let data = event
                        .lines()
                        .filter_map(|line| line.strip_prefix("data:"))
                        .map(str::trim_start)
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !data.is_empty() {
                        return serde_json::from_str(&data).unwrap();
                    }
                    continue;
                }
                let chunk = self.body.next().await.expect("event stream ended").unwrap();
                self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
            }
        })
        .await
        .expect("no event arrived")
    }
}

/// An ACP client speaking through the SDK's HTTP and WebSocket client, which
/// opens the session-scoped streams itself.
struct Peer {
    channel: Channel,
    driver: tokio::task::JoinHandle<agent_client_protocol::Result<()>>,
    next_id: u64,
    updates: Vec<Value>,
    permission_requests: Vec<Value>,
    permission_answer: &'static str,
}

impl Peer {
    fn connect(endpoint: &str, token: &str) -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        let client = HttpClient::builder_with_endpoint(endpoint)
            .configure_http(|http| http.no_proxy().default_headers(headers))
            .build()
            .unwrap();
        let (channel, driver) = ConnectTo::<Client>::into_channel_and_future(client);
        Self {
            channel,
            driver: tokio::spawn(driver.expect("the HTTP client drives its transport")),
            next_id: 1,
            updates: Vec::new(),
            permission_requests: Vec::new(),
            permission_answer: "allow_once",
        }
    }

    fn send(&mut self, message: Value) {
        self.channel
            .tx
            .unbounded_send(TransportFrame::parse_json(&message.to_string()))
            .unwrap();
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let message = self.next_message().await;
            if message["id"] == id {
                if let Some(error) = message.get("error") {
                    panic!("{method} failed: {error}");
                }
                return message["result"].clone();
            }
            assert_eq!(message["method"], "session/update", "{message}");
            self.updates.push(message);
        }
    }

    async fn next_message(&mut self) -> Value {
        loop {
            let frame = tokio::time::timeout(TIMEOUT, self.channel.rx.next())
                .await
                .expect("the agent stopped responding")
                .expect("the connection closed");
            let message: Value = serde_json::from_str(&frame.to_json().unwrap()).unwrap();
            if message["method"] == "session/request_permission" {
                self.permission_requests.push(message.clone());
                self.send(json!({
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {"outcome": {"outcome": "selected", "optionId": self.permission_answer}}
                }));
                continue;
            }
            return message;
        }
    }

    /// Close the connection the way a client exits, and wait for it to end.
    async fn close(self) {
        let Self {
            channel, driver, ..
        } = self;
        drop(channel);
        let _ = tokio::time::timeout(TIMEOUT, driver)
            .await
            .expect("the client transport did not finish");
    }

    fn streamed_text(&self) -> String {
        self.updates
            .iter()
            .filter(|update| update["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|update| update["params"]["update"]["content"]["text"].as_str())
            .collect()
    }
}

fn initialize_request(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {"protocolVersion": 1, "clientCapabilities": {}}
    })
}

#[tokio::test]
async fn a_prompt_turn_runs_over_streamable_http() {
    prompt_turn_with_permission(Transport::Http).await;
}

#[tokio::test]
async fn a_prompt_turn_runs_over_websocket() {
    prompt_turn_with_permission(Transport::WebSocket).await;
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    Http,
    WebSocket,
}

/// A v1 turn whose tool call needs permission, then a second connection that
/// loads the session the first one created.
async fn prompt_turn_with_permission(transport: Transport) {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, model)) =
        mock_sequence_server(vec![write_file_tool_call(), text_stream()]).await
    else {
        eprintln!("skipping {transport:?} prompt test: loopback networking is unavailable");
        return;
    };
    let server = Server::start(temp.path(), Some(&base_url), &["--permission-mode", "ask"]).await;
    let endpoint = match transport {
        Transport::Http => server.http_url(),
        Transport::WebSocket => server.ws_url(),
    };
    let cwd = temp.path().to_string_lossy().to_string();

    let mut peer = Peer::connect(&endpoint, &server.token);
    let initialize = peer
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    assert_eq!(initialize["protocolVersion"], 1);
    let session = peer
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = session["sessionId"].as_str().unwrap().to_string();
    let response = peer
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "write it"}]}),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(peer.permission_requests.len(), 1);
    assert_eq!(
        peer.permission_requests[0]["params"]["sessionId"],
        session_id
    );
    assert_eq!(peer.streamed_text(), "mock answer");
    assert_eq!(
        std::fs::read_to_string(temp.path().join("sample.txt")).unwrap(),
        "written by tool"
    );
    assert_eq!(model.await.unwrap().len(), 2);
    peer.close().await;

    // The next connection starts with nothing in memory, as a new stdio
    // process would, and loads the session from disk.
    let mut peer = Peer::connect(&endpoint, &server.token);
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await;
    peer.request(
        "session/load",
        json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
    )
    .await;
    assert_eq!(peer.streamed_text(), "mock answer");
    assert!(
        peer.updates
            .iter()
            .any(
                |update| update["params"]["update"]["sessionUpdate"] == "user_message_chunk"
                    && update["params"]["update"]["content"]["text"] == "write it"
            )
    );
    peer.close().await;
    assert!(!server.stderr().contains(&server.token));
}

#[tokio::test]
async fn a_v2_prompt_turn_runs_over_streamable_http() {
    v2_prompt_turn(Transport::Http).await;
}

#[tokio::test]
async fn a_v2_prompt_turn_runs_over_websocket() {
    v2_prompt_turn(Transport::WebSocket).await;
}

async fn v2_prompt_turn(transport: Transport) {
    let temp = tempfile::tempdir().unwrap();
    let Some((base_url, model)) = mock_sequence_server(vec![text_stream()]).await else {
        eprintln!("skipping v2 {transport:?} prompt test: loopback networking is unavailable");
        return;
    };
    let server = Server::start(temp.path(), Some(&base_url), &[]).await;
    let endpoint = match transport {
        Transport::Http => server.http_url(),
        Transport::WebSocket => server.ws_url(),
    };
    let mut peer = Peer::connect(&endpoint, &server.token);
    let initialize = peer
        .request(
            "initialize",
            json!({
                "protocolVersion": 2,
                "info": {"name": "http-test-client", "version": "1.0.0"},
                "capabilities": {}
            }),
        )
        .await;
    assert_eq!(initialize["protocolVersion"], 2);
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let accepted = peer
        .request(
            "session/prompt",
            json!({"sessionId": session["sessionId"], "prompt": [{"type": "text", "text": "hi"}]}),
        )
        .await;
    assert!(accepted["messageId"].as_str().is_some());
    let idle = tokio::time::timeout(TIMEOUT, async {
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
    .expect("the v2 turn should settle to idle");
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert_eq!(model.await.unwrap().len(), 1);
    peer.close().await;
}

#[tokio::test]
async fn requests_without_the_bearer_token_are_refused_before_acp() {
    let temp = tempfile::tempdir().unwrap();
    if loopback_listener().await.is_none() {
        eprintln!("skipping bearer token test: loopback networking is unavailable");
        return;
    }
    // Tracing is on, and two of the wrong tokens contain the real one, so a
    // log line carrying any of them would be caught below.
    let server = Server::start(temp.path(), None, &["--trace"]).await;
    assert!(
        server.token.len() >= 32,
        "a generated token is printed: {}",
        server.announcement
    );
    let wrong = [
        None,
        Some("Bearer wrong-token".to_string()),
        Some(format!("Basic {}", server.token)),
        Some(format!("Bearer {}x", server.token)),
        Some(server.token.clone()),
    ];
    let unknown_path = format!("http://{}/health", server.address);
    for authorization in &wrong {
        let with = |request: reqwest::RequestBuilder| match authorization {
            Some(value) => request.header(header::AUTHORIZATION, value),
            None => request,
        };
        let requests = [
            with(server.post(&initialize_request(1))),
            with(server.upgrade()),
            with(
                server
                    .client()
                    .get(server.http_url())
                    .header(header::ACCEPT, "text/event-stream")
                    .header("acp-connection-id", "unknown"),
            ),
            with(server.client().delete(server.http_url())),
            with(server.client().get(&unknown_path)),
        ];
        for request in requests {
            let response = request.send().await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{authorization:?}"
            );
            assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
            assert!(response.headers().get("acp-connection-id").is_none());
        }
    }
    let health = server
        .client()
        .get(&unknown_path)
        .header(header::AUTHORIZATION, server.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(
        health.status(),
        StatusCode::NOT_FOUND,
        "there is no health endpoint"
    );

    // None of that created a connection, so this one is admitted at once.
    let connection_id = server.initialize().await;
    // A request on the connection with a wrong token is not processed: had
    // `session/new` run, its response would be queued ahead of the list's.
    let rejected = server
        .post(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {"cwd": temp.path().to_string_lossy(), "mcpServers": []}
        }))
        .header(header::AUTHORIZATION, "Bearer wrong-token")
        .header("acp-connection-id", &connection_id)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    let rejected_delete = server
        .client()
        .delete(server.http_url())
        .header("acp-connection-id", &connection_id)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected_delete.status(), StatusCode::UNAUTHORIZED);
    let accepted = server
        .post(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/list", "params": {}}))
        .header(header::AUTHORIZATION, server.bearer())
        .header("acp-connection-id", &connection_id)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let mut events = server.events(&connection_id).await;
    let listed = events.next_message().await;
    assert_eq!(listed["id"], 3, "{listed}");
    assert_eq!(listed["result"]["sessions"], json!([]));
    let sessions = temp.path().join("sessions-data").join("sessions");
    assert_eq!(std::fs::read_dir(sessions).unwrap().count(), 0);
    server.delete(&connection_id).await;
    assert!(!server.stderr().contains(&server.token));
}

#[tokio::test]
async fn requests_for_another_host_or_from_a_browser_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    if loopback_listener().await.is_none() {
        eprintln!("skipping Host and Origin test: loopback networking is unavailable");
        return;
    }
    let server = Server::start(temp.path(), None, &[]).await;
    let port = server.address.port();
    let response = server
        .post(&initialize_request(1))
        .header(header::HOST, format!("rebind.example:{port}"))
        .header(header::AUTHORIZATION, server.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    // A DNS rebinding page reaches the server under its own name.
    let rebound = |http2: bool| {
        let builder = reqwest::Client::builder()
            .no_proxy()
            .resolve("rebind.example", server.address);
        let builder = if http2 {
            builder.http2_prior_knowledge()
        } else {
            builder
        };
        builder.build().unwrap()
    };
    for http2 in [false, true] {
        let response = rebound(http2)
            .post(format!("http://rebind.example:{port}/acp"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, server.bearer())
            .body(initialize_request(1).to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "http2: {http2}");
    }
    let response = rebound(false)
        .get(format!("http://rebind.example:{port}/acp"))
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(header::AUTHORIZATION, server.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    for origin in ["http://localhost:3000", "null", "https://evil.example"] {
        for request in [server.post(&initialize_request(1)), server.upgrade()] {
            let response = request
                .header(header::ORIGIN, origin)
                .header(header::AUTHORIZATION, server.bearer())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{origin}");
        }
    }

    // Loopback names on any port pass, as through an SSH port forward.
    let forwarded = reqwest::Client::builder()
        .no_proxy()
        .resolve("localhost", server.address)
        .build()
        .unwrap()
        .post(format!("http://localhost:{port}/acp"))
        .header(header::HOST, "localhost:9")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, server.bearer())
        .body(initialize_request(1).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(forwarded.status(), StatusCode::OK);
    let connection_id = forwarded.headers()["acp-connection-id"]
        .to_str()
        .unwrap()
        .to_string();
    server.delete(&connection_id).await;

    // Streamable HTTP over HTTP/2 (prior knowledge, as the RFD requires
    // HTTP/2) is served under the same checks.
    let response = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(server.http_url())
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, server.bearer())
        .body(initialize_request(1).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), reqwest::Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn one_acp_connection_is_served_at_a_time() {
    let temp = tempfile::tempdir().unwrap();
    if loopback_listener().await.is_none() {
        eprintln!("skipping single-connection test: loopback networking is unavailable");
        return;
    }
    let server = Server::start(temp.path(), None, &[]).await;
    // A rejected initialize answers on its POST and leaves the slot free.
    let mut unsupported = initialize_request(1);
    unsupported["params"]["protocolVersion"] = json!(0);
    let response = server
        .post(&unsupported)
        .header(header::AUTHORIZATION, server.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("acp-connection-id").is_none());
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32022, "{body}");
    // Over WebSocket the error arrives and only that connection ends.
    let mut rejected = Peer::connect(&server.ws_url(), &server.token);
    rejected.send(unsupported);
    let reply = rejected.next_message().await;
    assert_eq!(reply["error"]["code"], -32022, "{reply}");
    let closed = tokio::time::timeout(TIMEOUT, rejected.channel.rx.next())
        .await
        .expect("the rejected connection should close");
    assert!(closed.is_none(), "{closed:?}");
    rejected.close().await;

    let mut peer = Peer::connect(&server.ws_url(), &server.token);
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await;
    let session = peer
        .request(
            "session/new",
            json!({"cwd": temp.path().to_string_lossy(), "mcpServers": []}),
        )
        .await;

    for request in [server.post(&initialize_request(1)), server.upgrade()] {
        let response = request
            .header(header::AUTHORIZATION, server.bearer())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(response.text().await.unwrap(), BUSY);
    }
    // The connection in use is unaffected.
    let listed = peer.request("session/list", json!({})).await;
    assert_eq!(listed["sessions"][0]["sessionId"], session["sessionId"]);

    // Once it closes, the next one is admitted.
    peer.close().await;
    let connection_id = server.initialize().await;
    let mut events = server.events(&connection_id).await;
    let refused = server
        .post(&initialize_request(1))
        .header(header::AUTHORIZATION, server.bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::CONFLICT);
    let ping = server
        .post(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/list", "params": {}}))
        .header(header::AUTHORIZATION, server.bearer())
        .header("acp-connection-id", &connection_id)
        .send()
        .await
        .unwrap();
    assert_eq!(ping.status(), StatusCode::ACCEPTED);
    assert_eq!(events.next_message().await["id"], 2);
    server.delete(&connection_id).await;
    let mut peer = Peer::connect(&server.http_url(), &server.token);
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await;
    peer.close().await;
}

#[tokio::test]
async fn the_next_connection_starts_with_no_state_from_the_last() {
    let temp = tempfile::tempdir().unwrap();
    let command = |id: &str| {
        encode_sse(&[
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"run_command","arguments":json!({"command":"sh","args":["-c","printf a"]}).to_string()}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ])
    };
    let Some((base_url, model)) = mock_sequence_server(vec![
        command("call-a"),
        text_stream(),
        command("call-a-again"),
        text_stream(),
    ])
    .await
    else {
        eprintln!("skipping connection state test: loopback networking is unavailable");
        return;
    };
    let server = Server::start(temp.path(), Some(&base_url), &["--permission-mode", "ask"]).await;
    let cwd = temp.path().to_string_lossy().to_string();
    let initialize = json!({"protocolVersion": 1, "clientCapabilities": {}});
    let prompt = |session_id: &Value| json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "run it"}]});

    let mut first = Peer::connect(&server.http_url(), &server.token);
    first.permission_answer = "allow_always";
    first.request("initialize", initialize.clone()).await;
    let session = first
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = session["sessionId"].clone();
    let response = first.request("session/prompt", prompt(&session_id)).await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(first.permission_requests.len(), 1);
    first
        .request("providers/disable", json!({"providerId": "deepseek"}))
        .await;
    let providers = first.request("providers/list", json!({})).await;
    assert!(
        providers["providers"][0]["current"].is_null(),
        "{providers}"
    );
    first.close().await;

    // A new stdio process would not know about either; neither does the
    // next connection.
    let mut second = Peer::connect(&server.ws_url(), &server.token);
    second.request("initialize", initialize).await;
    let providers = second.request("providers/list", json!({})).await;
    assert!(
        providers["providers"][0]["current"].is_object(),
        "{providers}"
    );
    second
        .request(
            "session/load",
            json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
        )
        .await;
    let response = second.request("session/prompt", prompt(&session_id)).await;
    assert_eq!(response["stopReason"], "end_turn");
    assert_eq!(
        second.permission_requests.len(),
        1,
        "the first connection's \"allow always\" stayed with it"
    );
    second.close().await;
    assert_eq!(model.await.unwrap().len(), 4);
}

#[tokio::test]
async fn listening_beyond_loopback_needs_the_allow_flag() {
    let temp = tempfile::tempdir().unwrap();
    for args in [
        &["--listen", "0.0.0.0:0"][..],
        &["--listen", "[::]:0"],
        &["--listen-allow-remote"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
            .args(args)
            .arg("--data-dir")
            .arg(temp.path().join("sessions-data"))
            .env_remove("DSH_ACP_LISTEN")
            .env_remove("DSH_ACP_LISTEN_ALLOW_REMOTE")
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("--listen-allow-remote"),
            "{args:?}: {stderr}"
        );
        assert!(!stderr.contains("serving ACP"), "{args:?}: {stderr}");
    }
    if loopback_listener().await.is_none() {
        return;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
        .arg("--data-dir")
        .arg(temp.path().join("sessions-data"))
        .env("DSH_ACP_LISTEN", "0.0.0.0:0")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!output.status.success(), "DSH_ACP_LISTEN is checked too");

    let server = Server::start(
        temp.path(),
        None,
        &["--listen", "0.0.0.0:0", "--listen-allow-remote"],
    )
    .await;
    assert!(
        server.address.ip().is_unspecified(),
        "{}",
        server.announcement
    );
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("http://127.0.0.1:{}/acp", server.address.port()))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, server.bearer())
        .body(initialize_request(1).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn the_token_comes_from_the_environment_and_stays_out_of_commands() {
    let temp = tempfile::tempdir().unwrap();
    let command = encode_sse(&[
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-env","type":"function","function":{"name":"run_command","arguments":json!({"command":"sh","args":["-c","echo marker=$DSH_ACP_TEST_MARKER token=${DSH_ACP_LISTEN_TOKEN-unset}"]}).to_string()}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
    ]);
    let Some((base_url, model)) = mock_sequence_server(vec![command, text_stream()]).await else {
        eprintln!("skipping environment token test: loopback networking is unavailable");
        return;
    };
    for short in ["too-short", "sixteen or more but spaced"] {
        let output = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
            .args(["--listen", "127.0.0.1:0", "--data-dir"])
            .arg(temp.path().join("sessions-data"))
            .env("DSH_ACP_LISTEN_TOKEN", short)
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(!output.status.success(), "{short:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("DSH_ACP_LISTEN_TOKEN must be"),
            "{short:?}"
        );
    }
    let server = Server::start_with(
        temp.path(),
        Some(&base_url),
        &["--permission-mode", "allow"],
        &[
            ("DSH_ACP_LISTEN_TOKEN", ENV_TOKEN),
            ("DSH_ACP_TEST_MARKER", "visible"),
        ],
    )
    .await;
    assert!(
        !server.announcement.contains(ENV_TOKEN),
        "a configured token is not printed: {}",
        server.announcement
    );
    let mut peer = Peer::connect(&server.http_url(), ENV_TOKEN);
    peer.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
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
            json!({"sessionId": session["sessionId"], "prompt": [{"type": "text", "text": "env"}]}),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    peer.close().await;
    let requests = model.await.unwrap();
    let tool_result = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .expect("the command's output went back to the model")
        .to_string();
    assert!(tool_result.contains("marker=visible"), "{tool_result}");
    assert!(tool_result.contains("token=unset"), "{tool_result}");
    assert!(!requests[1].to_string().contains(ENV_TOKEN));
    assert!(!server.stderr().contains(ENV_TOKEN));
}

// ---------------------------------------------------------------------------
// Mock DeepSeek API, as in acp_stdio.rs.
// ---------------------------------------------------------------------------

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

fn write_file_tool_call() -> String {
    encode_sse(&[
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"write_file","arguments":"{\"path\":\"sample.txt\",\"content\":\"written by tool\"}"}}]}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}),
    ])
}

fn text_stream() -> String {
    encode_sse(&[
        json!({"choices":[{"delta":{"content":"mock answer"}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":2,"total_tokens":22}}),
    ])
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
