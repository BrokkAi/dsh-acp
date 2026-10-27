use std::{
    process::Stdio,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard},
    time::Duration,
};

use agent_client_protocol::{
    Error,
    schema::v1::{EnvVariable, HttpHeader, McpServer},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

use crate::tools::function_tool;

#[derive(Debug, Clone)]
pub struct McpToolMetadata {
    pub qualified_name: String,
    server_name: String,
    pub original_name: String,
    pub description: String,
    pub input_schema: Value,
    pub read_only: bool,
}

impl McpToolMetadata {
    fn new(server_name: &str, tool: McpToolDescription) -> Self {
        let qualified_name = qualify_tool_name(server_name, &tool.name);
        let description = tool
            .description
            .unwrap_or_else(|| format!("MCP tool {} from {}", tool.name, server_name));
        let input_schema = tool
            .input_schema
            .unwrap_or_else(|| json!({"type": "object", "additionalProperties": true}));
        Self {
            qualified_name,
            server_name: server_name.to_string(),
            original_name: tool.name,
            description,
            input_schema,
            read_only: tool.read_only,
        }
    }
}

/// The MCP servers of one session. The session hands out clones of its `Arc`
/// so a call or refresh runs without the session state lock; each client
/// serializes its own requests.
pub struct McpSession {
    clients: Vec<Arc<McpClient>>,
    /// Its lock is never held across an await.
    tools: RwLock<Vec<McpToolMetadata>>,
}

impl std::fmt::Debug for McpSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpSession")
            .field("clients", &self.clients.len())
            .field("tools", &self.tools)
            .finish()
    }
}

impl McpSession {
    /// Connects each server and lists its tools. A server gets
    /// `setup_timeout` for its handshake and first `tools/list`, because the
    /// lifecycle request waiting on it holds up every later message on the
    /// connection. If any server fails, the stdio processes started so far
    /// are killed and reaped before the error is returned.
    pub async fn connect(
        servers: &[McpServer],
        setup_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<Self, Error> {
        let mut session = Self {
            clients: Vec::new(),
            tools: RwLock::new(Vec::new()),
        };
        for server in servers {
            if let Err(error) = session
                .add_server(server, setup_timeout, cancellation)
                .await
            {
                for client in &session.clients {
                    client.shutdown().await;
                }
                return Err(error);
            }
        }
        Ok(session)
    }

    async fn add_server(
        &mut self,
        server: &McpServer,
        setup_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let requested_name = match server {
            McpServer::Stdio(config) => &config.name,
            McpServer::Http(config) => &config.name,
            McpServer::Sse(config) => &config.name,
            McpServer::Acp(config) => &config.name,
            _ => "mcp",
        };
        let name = unique_server_name(requested_name, self.clients.as_slice());
        let client = Arc::new(McpClient::start(server, name)?);
        self.clients.push(client.clone());
        // One deadline for the whole setup rather than per read, so a server
        // that keeps writing unrelated lines cannot extend it.
        let setup = async {
            client.initialize(requested_name, cancellation).await?;
            client.list_tools(cancellation).await
        };
        let tools = tokio::time::timeout(setup_timeout, setup)
            .await
            .map_err(|_| {
                Error::internal_error().data(format!(
                    "MCP server `{requested_name}` did not connect and list its tools within {} seconds. If it needs longer to start, raise DSH_ACP_MCP_CONNECT_TIMEOUT_SECS (or --mcp-connect-timeout-secs).",
                    setup_timeout.as_secs()
                ))
            })??;
        self.tools
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(
                tools
                    .into_iter()
                    .map(|tool| McpToolMetadata::new(client.name(), tool)),
            );
        Ok(())
    }

    pub async fn refresh(&self, cancellation: &CancellationToken) -> Result<(), Error> {
        let mut tools = Vec::new();
        for client in &self.clients {
            for tool in client.list_tools(cancellation).await? {
                tools.push(McpToolMetadata::new(client.name(), tool));
            }
        }
        *self.tools.write().unwrap_or_else(PoisonError::into_inner) = tools;
        Ok(())
    }

    fn tools(&self) -> RwLockReadGuard<'_, Vec<McpToolMetadata>> {
        self.tools.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn definitions(&self) -> Vec<Value> {
        self.tools()
            .iter()
            .map(|tool| {
                function_tool(
                    &tool.qualified_name,
                    &tool.description,
                    tool.input_schema.clone(),
                )
            })
            .collect()
    }

    pub fn has_tool(&self, qualified_name: &str) -> bool {
        self.tools()
            .iter()
            .any(|tool| tool.qualified_name == qualified_name)
    }

    pub fn is_read_only(&self, qualified_name: &str) -> bool {
        self.tools()
            .iter()
            .any(|tool| tool.qualified_name == qualified_name && tool.read_only)
    }

    pub fn read_only_names(&self) -> Vec<String> {
        self.tools()
            .iter()
            .filter(|tool| tool.read_only)
            .map(|tool| tool.qualified_name.clone())
            .collect()
    }

    pub async fn call(
        &self,
        qualified_name: &str,
        arguments: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        let (server_name, original_name) = {
            let tools = self.tools();
            let metadata = tools
                .iter()
                .find(|tool| tool.qualified_name == qualified_name)
                .ok_or_else(|| Error::invalid_params().data("unknown MCP tool"))?;
            (metadata.server_name.clone(), metadata.original_name.clone())
        };
        let client = self
            .clients
            .iter()
            .find(|client| client.name() == server_name)
            .ok_or_else(|| Error::internal_error().data("MCP server route is missing"))?;
        client
            .call_tool(&original_name, arguments, cancellation)
            .await
    }
}

fn unique_server_name(requested: &str, clients: &[Arc<McpClient>]) -> String {
    let base = sanitize_component(requested);
    if clients.iter().all(|client| client.name() != base) {
        return base;
    }
    for suffix in 2.. {
        let candidate = format!("{base}_{suffix}");
        if clients.iter().all(|client| client.name() != candidate) {
            return candidate;
        }
    }
    unreachable!("unbounded server-name suffix search")
}

pub fn mcp_result_text(value: &Value) -> String {
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        let mut pieces = Vec::new();
        for item in content {
            match item.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        pieces.push(text.to_string());
                    }
                }
                Some("resource") => {
                    if let Some(text) = item.pointer("/resource/text").and_then(Value::as_str) {
                        pieces.push(text.to_string());
                    } else {
                        pieces.push(item.to_string());
                    }
                }
                _ => pieces.push(item.to_string()),
            }
        }
        if !pieces.is_empty() {
            return pieces.join("\n");
        }
    }
    if let Some(structured) = value.get("structuredContent") {
        return serde_json::to_string_pretty(structured).unwrap_or_else(|_| structured.to_string());
    }
    value.to_string()
}

pub fn qualify_tool_name(server: &str, tool: &str) -> String {
    let mut name = format!(
        "mcp__{}__{}",
        sanitize_component(server),
        sanitize_component(tool)
    );
    if name.len() > 64 {
        let digest = fnv1a(name.as_bytes());
        name.truncate(55);
        name.push_str(&format!("_{digest:08x}"));
    }
    name
}

fn sanitize_component(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
            output.push(character);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "unnamed".to_string()
    } else {
        output
    }
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[derive(Clone, Debug)]
struct McpToolDescription {
    name: String,
    description: Option<String>,
    input_schema: Option<Value>,
    read_only: bool,
}

struct McpClient {
    name: String,
    transport: McpTransport,
}

enum McpTransport {
    Stdio(Mutex<StdioTransport>),
    Http(Mutex<HttpTransport>),
}

impl McpClient {
    /// Spawns a stdio server or prepares an HTTP client, without talking to
    /// the server yet.
    fn start(server: &McpServer, name: String) -> Result<Self, Error> {
        let transport = match server {
            McpServer::Stdio(config) => McpTransport::Stdio(Mutex::new(StdioTransport::spawn(
                &config.command,
                &config.args,
                &config.env,
            )?)),
            McpServer::Http(config) => McpTransport::Http(Mutex::new(HttpTransport::new(
                &config.url,
                &config.headers,
            )?)),
            McpServer::Sse(config) => {
                let mut transport = HttpTransport::new(&config.url, &config.headers)?;
                transport.prefer_streamable_http = true;
                McpTransport::Http(Mutex::new(transport))
            }
            McpServer::Acp(config) => {
                return Err(Error::invalid_params().data(format!(
                    "MCP-over-ACP server `{}` requires proxy attachment support, which this standalone agent does not provide",
                    config.name
                )));
            }
            _ => return Err(Error::invalid_params().data("unsupported MCP server transport")),
        };
        Ok(Self { name, transport })
    }

    async fn initialize(&self, name: &str, cancellation: &CancellationToken) -> Result<(), Error> {
        match &self.transport {
            McpTransport::Stdio(transport) => {
                transport.lock().await.initialize(name, cancellation).await
            }
            McpTransport::Http(transport) => {
                transport.lock().await.initialize(name, cancellation).await
            }
        }
    }

    /// Kills a stdio server and waits for it to exit. Dropping the transport
    /// only starts the kill.
    async fn shutdown(&self) {
        if let McpTransport::Stdio(transport) = &self.transport {
            let _ = transport.lock().await.child.kill().await;
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn list_tools(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Vec<McpToolDescription>, Error> {
        let mut tools = Vec::new();
        let mut cursor = None::<String>;
        loop {
            let params = cursor
                .as_ref()
                .map_or_else(|| json!({}), |cursor| json!({"cursor": cursor}));
            let response = match &self.transport {
                McpTransport::Stdio(transport) => {
                    transport
                        .lock()
                        .await
                        .request("tools/list", params, cancellation)
                        .await?
                }
                McpTransport::Http(transport) => {
                    transport
                        .lock()
                        .await
                        .request("tools/list", params, cancellation)
                        .await?
                }
            };
            let (mut page, next_cursor) = parse_tool_list_page(&response)?;
            tools.append(&mut page);
            match next_cursor {
                Some(next) if !next.is_empty() => cursor = Some(next),
                _ => break,
            }
        }
        Ok(tools)
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        let params = json!({"name": name, "arguments": arguments});
        match &self.transport {
            McpTransport::Stdio(transport) => {
                transport
                    .lock()
                    .await
                    .request("tools/call", params, cancellation)
                    .await
            }
            McpTransport::Http(transport) => {
                transport
                    .lock()
                    .await
                    .request("tools/call", params, cancellation)
                    .await
            }
        }
    }
}

#[cfg(test)]
fn parse_tool_list(value: &Value) -> Result<Vec<McpToolDescription>, Error> {
    parse_tool_list_page(value).map(|(tools, _)| tools)
}

fn parse_tool_list_page(value: &Value) -> Result<(Vec<McpToolDescription>, Option<String>), Error> {
    let tools = value
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::internal_error().data("MCP tools/list response has no tools array")
        })?;
    let tools = tools
        .iter()
        .filter_map(|tool| {
            Some(McpToolDescription {
                name: tool.get("name")?.as_str()?.to_string(),
                description: tool
                    .get("description")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                input_schema: tool.get("inputSchema").cloned(),
                read_only: tool
                    .get("annotations")
                    .and_then(|annotations| annotations.get("readOnlyHint"))
                    .and_then(Value::as_bool)
                    == Some(true),
            })
        })
        .collect();
    let next_cursor = value
        .get("nextCursor")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Ok((tools, next_cursor))
}

struct StdioTransport {
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    child: Child,
    next_id: u64,
}

impl StdioTransport {
    fn spawn(
        command: &std::path::Path,
        args: &[String],
        env: &[EnvVariable],
    ) -> Result<Self, Error> {
        let mut process = tokio::process::Command::new(command);
        process
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for variable in env {
            process.env(&variable.name, &variable.value);
        }
        let mut child = process.spawn().map_err(Error::into_internal_error)?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::internal_error().data("MCP process stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::internal_error().data("MCP process stdout unavailable"))?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut buffer = [0u8; 4096];
                while let Ok(count) = stderr.read(&mut buffer).await {
                    if count == 0 {
                        break;
                    }
                    tracing::debug!(
                        target: "dsh_acp::mcp",
                        "{}",
                        String::from_utf8_lossy(&buffer[..count]).trim_end()
                    );
                }
            });
        }
        Ok(Self {
            stdin,
            stdout: BufReader::new(stdout),
            child,
            next_id: 1,
        })
    }

    async fn initialize(
        &mut self,
        name: &str,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let _ = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "dsh-acp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                cancellation,
            )
            .await?;
        self.notify("notifications/initialized", json!({}), cancellation)
            .await?;
        tracing::debug!(
            target: "dsh_acp::mcp",
            mcp_server = name,
            "connected stdio MCP server"
        );
        Ok(())
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.write(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await?;

        let mut line = String::new();
        loop {
            line.clear();
            let read = tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
                result = self.stdout.read_line(&mut line) => result.map_err(Error::into_internal_error)?,
            };
            if read == 0 {
                return Err(Error::internal_error().data(format!(
                    "MCP server exited with status {:?}",
                    self.child
                        .try_wait()
                        .ok()
                        .flatten()
                        .and_then(|status| status.code())
                )));
            }
            let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            return extract_mcp_response(value);
        }
    }

    async fn notify(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        tokio::select! {
            _ = cancellation.cancelled() => Err(Error::request_cancelled()),
            result = self.write(json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params
            })) => result,
        }
    }

    async fn write(&mut self, value: Value) -> Result<(), Error> {
        let mut bytes = serde_json::to_vec(&value).map_err(Error::into_internal_error)?;
        bytes.push(b'\n');
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(Error::into_internal_error)?;
        self.stdin.flush().await.map_err(Error::into_internal_error)
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

struct HttpTransport {
    client: reqwest::Client,
    url: reqwest::Url,
    headers: Vec<HttpHeader>,
    session_id: Option<String>,
    negotiate_streamable: bool,
    prefer_streamable_http: bool,
    protocol_initialized: bool,
    next_id: u64,
}

impl HttpTransport {
    fn new(url: &str, headers: &[HttpHeader]) -> Result<Self, Error> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| Error::invalid_params().data(error.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::invalid_params().data("MCP URL must use http or https"));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(180))
            .user_agent(concat!("dsh-acp/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(Error::into_internal_error)?;
        Ok(Self {
            client,
            url,
            headers: headers.to_vec(),
            session_id: None,
            negotiate_streamable: false,
            prefer_streamable_http: false,
            protocol_initialized: false,
            next_id: 1,
        })
    }

    async fn initialize(
        &mut self,
        name: &str,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        self.negotiate_streamable = true;
        let _ = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "dsh-acp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                cancellation,
            )
            .await?;
        self.protocol_initialized = true;
        self.notify("notifications/initialized", json!({}), cancellation)
            .await?;
        tracing::debug!(
            target: "dsh_acp::mcp",
            mcp_server = name,
            "connected HTTP MCP server"
        );
        Ok(())
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let value = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        let response = self.post(value, cancellation).await?;
        extract_mcp_response(response)
    }

    async fn notify(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let _ = self
            .post(
                json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params
                }),
                cancellation,
            )
            .await?;
        Ok(())
    }

    async fn post(
        &mut self,
        body: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        let mut request = self
            .client
            .post(self.url.clone())
            .header(
                reqwest::header::ACCEPT,
                if self.negotiate_streamable || self.prefer_streamable_http {
                    "application/json, text/event-stream"
                } else {
                    "application/json"
                },
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body);
        for header in &self.headers {
            request = request.header(&header.name, &header.value);
        }
        if let Some(session_id) = &self.session_id {
            request = request.header("Mcp-Session-Id", session_id);
        }
        if self.protocol_initialized {
            request = request.header("MCP-Protocol-Version", "2025-06-18");
        }

        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
            response = request.send() => response.map_err(Error::into_internal_error)?,
        };
        if let Some(session_id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
        {
            self.session_id = Some(session_id.to_string());
        }
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(Error::internal_error().data(json!({
                "status": status.as_u16(),
                "message": text
            })));
        }
        if body.get("id").is_none() {
            return Ok(Value::Null);
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if content_type.contains("text/event-stream") {
            read_mcp_sse_response(response, cancellation, body.get("id").cloned()).await
        } else {
            tokio::select! {
                _ = cancellation.cancelled() => Err(Error::request_cancelled()),
                response = response.json::<Value>() => response.map_err(Error::into_internal_error),
            }
        }
    }
}

async fn read_mcp_sse_response(
    response: reqwest::Response,
    cancellation: &CancellationToken,
    expected_id: Option<Value>,
) -> Result<Value, Error> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::<u8>::new();
    let mut data = String::new();
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(Error::into_internal_error)?;
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let raw = buffer.drain(..=newline).collect::<Vec<_>>();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !data.is_empty() {
                    if let Ok(value) = serde_json::from_str::<Value>(&data)
                        && expected_id
                            .as_ref()
                            .is_none_or(|expected| value.get("id") == Some(expected))
                    {
                        return Ok(value);
                    }
                    data.clear();
                }
                continue;
            }
            if let Some(payload) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(payload.trim_start());
            }
        }
    }
    if !data.is_empty() {
        return serde_json::from_str(&data).map_err(Error::into_internal_error);
    }
    Err(Error::internal_error().data("MCP SSE response ended without a JSON-RPC payload"))
}

fn extract_mcp_response(value: Value) -> Result<Value, Error> {
    if let Some(error) = value.get("error") {
        return Err(Error::internal_error().data(error.clone()));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| Error::internal_error().data("MCP response has no result"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_names_are_protocol_safe_and_deterministic() {
        let first = qualify_tool_name("my server", "read/file");
        let second = qualify_tool_name("my server", "read/file");
        assert_eq!(first, second);
        assert!(first.starts_with("mcp__my_server__read_file"));
        assert!(first.len() <= 64);
    }

    #[test]
    fn formats_mcp_content() {
        assert_eq!(
            mcp_result_text(&json!({
                "content": [
                    {"type": "text", "text": "one"},
                    {"type": "text", "text": "two"}
                ]
            })),
            "one\ntwo"
        );
    }

    #[test]
    fn parses_common_tool_list() {
        let tools = parse_tool_list(&json!({
            "tools": [{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]
        }))
        .unwrap();
        assert_eq!(tools[0].name, "echo");
    }

    #[test]
    fn parses_read_only_hint_only_when_true_boolean() {
        let tools = parse_tool_list(&json!({"tools": [
            {"name":"true", "annotations":{"readOnlyHint":true}},
            {"name":"false", "annotations":{"readOnlyHint":false}},
            {"name":"absent"},
            {"name":"string", "annotations":{"readOnlyHint":"true"}},
            {"name":"number", "annotations":{"readOnlyHint":1}}
        ]}))
        .unwrap();
        assert_eq!(
            tools.iter().map(|tool| tool.read_only).collect::<Vec<_>>(),
            [true, false, false, false, false]
        );
    }

    /// A stdio server that runs `script` with `/bin/sh`, with `args` as `$1`
    /// onwards.
    #[cfg(unix)]
    fn shell_server(name: &str, script: &str, args: &[&str]) -> McpServer {
        let mut argv = vec!["-c".to_string(), script.to_string(), "sh".to_string()];
        argv.extend(args.iter().map(ToString::to_string));
        McpServer::Stdio(
            agent_client_protocol::schema::v1::McpServerStdio::new(name, "/bin/sh").args(argv),
        )
    }

    /// Whether the process whose PID a test server wrote still exists.
    /// `kill -0` also succeeds for an exited process nobody has reaped.
    #[cfg(unix)]
    fn process_exists(pid_file: &std::path::Path) -> bool {
        let pid = std::fs::read_to_string(pid_file).expect("the MCP server should have started");
        std::process::Command::new("/bin/sh")
            .args(["-c", "kill -0 \"$1\" 2>/dev/null", "sh", pid.trim()])
            .status()
            .unwrap()
            .success()
    }

    /// Connects with a short setup timeout, failing the test instead of
    /// hanging it if that timeout does not fire.
    async fn connect_error(server: McpServer) -> String {
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            McpSession::connect(
                &[server],
                Duration::from_millis(300),
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("MCP setup should time out")
        .expect_err("MCP setup should fail");
        error
            .data
            .as_ref()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stdio_server_lists_tools_past_unrelated_output() {
        let script = r#"
            read -r line
            echo 'starting up'
            echo '{"jsonrpc":"2.0","method":"notifications/message","params":{}}'
            echo '{"jsonrpc":"2.0","id":1,"result":{}}'
            read -r line
            read -r line
            echo '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo"}]}}'
            while read -r line; do :; done
        "#;
        let session = McpSession::connect(
            &[shell_server("ok", script, &[])],
            Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(session.has_tool("mcp__ok__echo"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn setup_timeout_covers_the_first_tool_list() {
        // Answers initialize, then keeps reading without answering again.
        let script = r#"
            read -r line
            echo '{"jsonrpc":"2.0","id":1,"result":{}}'
            while read -r line; do :; done
        "#;
        let message = connect_error(shell_server("slow", script, &[])).await;
        assert!(message.contains("MCP server `slow`"), "{message}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn output_that_never_answers_does_not_extend_the_setup_timeout() {
        let script = r#"
            while :; do
                echo 'not JSON-RPC'
                echo '{"jsonrpc":"2.0","method":"notifications/message","params":{}}'
                echo '{"jsonrpc":"2.0","id":99,"result":{}}'
            done
        "#;
        let message = connect_error(shell_server("chatty", script, &[])).await;
        assert!(message.contains("MCP server `chatty`"), "{message}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_setup_kills_and_reaps_every_stdio_server_before_returning() {
        let temp = tempfile::tempdir().unwrap();
        let ready_pid = temp.path().join("ready.pid");
        let silent_pid = temp.path().join("silent.pid");
        let ready = r#"
            echo $$ > "$1"
            read -r line
            echo '{"jsonrpc":"2.0","id":1,"result":{}}'
            read -r line
            read -r line
            echo '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}'
            while read -r line; do :; done
        "#;
        let servers = [
            shell_server("ready", ready, &[ready_pid.to_str().unwrap()]),
            shell_server(
                "silent",
                r#"echo $$ > "$1"; exec sleep 60"#,
                &[silent_pid.to_str().unwrap()],
            ),
        ];
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            McpSession::connect(
                &servers,
                Duration::from_millis(500),
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("MCP setup should time out")
        .expect_err("the silent server should fail setup");
        // Nothing is awaited between the error and these checks. The test
        // runtime has one thread, so tokio's background reaper cannot run
        // first: only an explicit kill-and-wait removes both processes.
        assert!(
            !process_exists(&ready_pid),
            "the connected server is still running"
        );
        assert!(
            !process_exists(&silent_pid),
            "the silent server is still running"
        );
        let message = error
            .data
            .as_ref()
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(message.contains("MCP server `silent`"), "{message}");
    }

    #[tokio::test]
    async fn setup_timeout_also_bounds_http_servers() {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping HTTP MCP timeout test: loopback networking is unavailable");
                return;
            }
            Err(error) => panic!("failed to bind loopback MCP server: {error}"),
        };
        let url = format!("http://{}", listener.local_addr().unwrap());
        // Accept connections and hold them open without answering.
        let server = tokio::spawn(async move {
            let mut sockets = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                sockets.push(socket);
            }
        });
        let message = connect_error(McpServer::Http(
            agent_client_protocol::schema::v1::McpServerHttp::new("remote", url),
        ))
        .await;
        assert!(message.contains("MCP server `remote`"), "{message}");
        server.abort();
    }
}
