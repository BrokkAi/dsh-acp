use std::{
    collections::HashSet,
    ffi::OsString,
    process::Stdio,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard},
    time::Duration,
};

use agent_client_protocol::{
    Client, ConnectionTo, Error, V2ConnectionTo,
    mcp_server::MCP_SERVER_UNAVAILABLE,
    schema::{
        v1::{self, EnvVariable, HttpHeader, McpServer},
        v2,
    },
};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

use crate::tools::function_tool;

/// The only MCP revision MCP-over-ACP carries. It is stateless: every request
/// names its version and capabilities, and there is no `initialize`.
const MCP_OVER_ACP_VERSION: &str = "2026-07-28";
const MCP_MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MCP_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MCP_MAX_TOOL_METADATA_BYTES: usize = 1024 * 1024;
const MCP_MAX_TOOLS_PER_PAGE: usize = 512;
const MCP_MAX_TOOLS: usize = 1024;
const MCP_MAX_TOOL_PAGES: usize = 100;
const MCP_MAX_CURSOR_BYTES: usize = 4096;

/// The ACP connection a session was set up on. Its client serves the
/// session's MCP-over-ACP servers, in that connection's protocol version.
#[derive(Clone)]
pub enum AcpConnection {
    V1(ConnectionTo<Client>),
    V2(V2ConnectionTo<Client>),
}

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
    ///
    /// MCP-over-ACP servers are reached through `acp` and are not contacted
    /// here: stateless MCP has no handshake, and the client cannot answer a
    /// `tools/list` until the lifecycle request that would wait for it
    /// returns. The turn's first [`refresh`](Self::refresh) lists their tools.
    pub async fn connect(
        servers: &[McpServer],
        acp: Option<&AcpConnection>,
        setup_timeout: Duration,
        allow_insecure_http: bool,
        cancellation: &CancellationToken,
    ) -> Result<Self, Error> {
        reject_duplicate_acp_servers(servers)?;
        let mut session = Self {
            clients: Vec::new(),
            tools: RwLock::new(Vec::new()),
        };
        for server in servers {
            if let Err(error) = session
                .add_server(
                    server,
                    acp,
                    setup_timeout,
                    allow_insecure_http,
                    cancellation,
                )
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
        acp: Option<&AcpConnection>,
        setup_timeout: Duration,
        allow_insecure_http: bool,
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
        let client = Arc::new(McpClient::start(server, name, acp, allow_insecure_http)?);
        self.clients.push(client.clone());
        if matches!(client.transport, McpTransport::Acp(_)) {
            return Ok(());
        }
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

    /// Lists every server's tools again. A server that fails keeps the tools
    /// its last listing found, none if it was never listed, so it cannot hide
    /// the other servers' tools. An MCP-over-ACP server whose client no longer
    /// serves it (`-33001`) loses its tools instead, because calling them
    /// could only fail. The error names each server that failed, after the
    /// others' tools are stored.
    pub async fn refresh(&self, cancellation: &CancellationToken) -> Result<(), Error> {
        let mut tools = Vec::new();
        let mut failures = Vec::new();
        for client in &self.clients {
            match client.list_tools(cancellation).await {
                Ok(listed) => tools.extend(
                    listed
                        .into_iter()
                        .map(|tool| McpToolMetadata::new(client.name(), tool)),
                ),
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
                    return Err(error);
                }
                Err(error) => {
                    if !registration_unavailable(&error) {
                        tools.extend(
                            self.tools()
                                .iter()
                                .filter(|tool| tool.server_name == client.name())
                                .cloned(),
                        );
                    }
                    failures.push(format!(
                        "MCP server `{}` could not list its tools: {}",
                        client.name(),
                        crate::tools::error_reason(&error)
                    ));
                }
            }
        }
        *self.tools.write().unwrap_or_else(PoisonError::into_inner) = tools;
        if failures.is_empty() {
            return Ok(());
        }
        let mut error = Error::internal_error().data(json!(failures));
        error.message = failures.join("; ");
        Err(error)
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

/// Whether the client answered that it no longer serves an MCP-over-ACP
/// server, rather than failing one request.
fn registration_unavailable(error: &Error) -> bool {
    error
        .data
        .as_ref()
        .and_then(|data| data.pointer("/acpError/code"))
        .and_then(Value::as_i64)
        == Some(i64::from(MCP_SERVER_UNAVAILABLE))
}

/// A `serverId` names one registration, so declaring it twice for a session
/// would only offer the same tools under two names.
fn reject_duplicate_acp_servers(servers: &[McpServer]) -> Result<(), Error> {
    let mut seen = HashSet::new();
    for server in servers {
        if let McpServer::Acp(config) = server
            && !seen.insert(&config.server_id)
        {
            return Err(Error::invalid_params().data(format!(
                "MCP-over-ACP serverId `{}` is declared more than once",
                config.server_id
            )));
        }
    }
    Ok(())
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
    /// Each request stands alone, so there is nothing to serialize.
    Acp(AcpTransport),
}

impl McpClient {
    /// Spawns a stdio server or prepares an HTTP or MCP-over-ACP client,
    /// without talking to the server yet.
    fn start(
        server: &McpServer,
        name: String,
        acp: Option<&AcpConnection>,
        allow_insecure_http: bool,
    ) -> Result<Self, Error> {
        let transport = match server {
            McpServer::Stdio(config) => McpTransport::Stdio(Mutex::new(StdioTransport::spawn(
                &config.command,
                &config.args,
                &config.env,
            )?)),
            McpServer::Http(config) => McpTransport::Http(Mutex::new(HttpTransport::new(
                &config.url,
                &config.headers,
                allow_insecure_http,
            )?)),
            McpServer::Sse(config) => {
                let mut transport =
                    HttpTransport::new(&config.url, &config.headers, allow_insecure_http)?;
                transport.prefer_streamable_http = true;
                McpTransport::Http(Mutex::new(transport))
            }
            McpServer::Acp(config) => McpTransport::Acp(AcpTransport {
                connection: acp.cloned().ok_or_else(|| {
                    Error::internal_error().data(format!(
                        "MCP-over-ACP server `{}` has no ACP connection",
                        config.name
                    ))
                })?,
                server_id: config.server_id.0.clone(),
            }),
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
            McpTransport::Acp(_) => Ok(()),
        }
    }

    async fn request(
        &self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        match &self.transport {
            McpTransport::Stdio(transport) => {
                transport
                    .lock()
                    .await
                    .request(method, params, cancellation)
                    .await
            }
            McpTransport::Http(transport) => {
                transport
                    .lock()
                    .await
                    .request(method, params, cancellation)
                    .await
            }
            McpTransport::Acp(transport) => transport.request(method, params, cancellation).await,
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
        let mut seen_cursors = HashSet::new();
        let mut pages = 0;
        loop {
            if pages >= MCP_MAX_TOOL_PAGES {
                return Err(Error::internal_error().data(format!(
                    "MCP tools/list exceeded the {MCP_MAX_TOOL_PAGES}-page limit"
                )));
            }
            pages += 1;
            let params = cursor
                .as_ref()
                .map_or_else(|| json!({}), |cursor| json!({"cursor": cursor}));
            let response = self.request("tools/list", params, cancellation).await?;
            let (mut page, next_cursor) = parse_tool_list_page(&response)?;
            if tools.len().saturating_add(page.len()) > MCP_MAX_TOOLS {
                return Err(Error::internal_error().data(format!(
                    "MCP tools/list exceeded the {MCP_MAX_TOOLS}-tool limit"
                )));
            }
            tools.append(&mut page);
            match next_cursor {
                Some(next) if !next.is_empty() => {
                    if !seen_cursors.insert(next.clone()) {
                        return Err(Error::internal_error().data(format!(
                            "MCP tools/list repeated pagination cursor `{next}`"
                        )));
                    }
                    cursor = Some(next);
                }
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
        self.request("tools/call", params, cancellation).await
    }
}

/// An MCP server the ACP client provides (MCP-over-ACP, draft RFD). Each MCP
/// operation is one `mcp/message` request to the client over the session's
/// ACP connection, carrying its own MCP 2026-07-28 metadata.
struct AcpTransport {
    connection: AcpConnection,
    server_id: Arc<str>,
}

/// The inner MCP outcome a successful `mcp/message` response carries.
enum AcpMcpOutcome {
    Result(Value),
    /// An MCP error object. Its code is MCP's, not ACP's.
    Error(Value),
}

impl AcpTransport {
    async fn request(
        &self,
        method: &str,
        params: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::request_cancelled());
        }
        let mut params = match params {
            Value::Object(params) => params,
            _ => Map::new(),
        };
        params.insert(
            "_meta".to_string(),
            json!({
                "io.modelcontextprotocol/protocolVersion": MCP_OVER_ACP_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {
                    "name": "dsh-acp",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        );
        // Fresh for every operation, so no two active requests to a server
        // on this connection share one.
        let request_id = format!("dsh-acp-{}", uuid::Uuid::new_v4().simple());
        // Dropping the pending request when the turn is cancelled sends the
        // client `$/cancel_request` for it.
        let outcome = match &self.connection {
            AcpConnection::V1(connection) => {
                let response = connection.send_request(
                    v1::MessageMcpRequest::new(self.server_id.clone(), request_id, method)
                        .params(params),
                );
                let response = tokio::select! {
                    _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
                    response = response.block_task() => response,
                };
                match response.map_err(|error| self.binding_failure(error))? {
                    v1::MessageMcpResponse::Result { result, .. } => AcpMcpOutcome::Result(result),
                    v1::MessageMcpResponse::Error { error, .. } => {
                        AcpMcpOutcome::Error(serde_json::to_value(error).unwrap_or_default())
                    }
                    _ => return Err(self.unrecognized_response()),
                }
            }
            AcpConnection::V2(connection) => {
                let response = connection.send_request(
                    v2::MessageMcpRequest::new(self.server_id.clone(), request_id, method)
                        .params(params),
                );
                let response = tokio::select! {
                    _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
                    response = response.block_task() => response,
                };
                match response.map_err(|error| self.binding_failure(error))? {
                    v2::MessageMcpResponse::Result { result, .. } => AcpMcpOutcome::Result(result),
                    v2::MessageMcpResponse::Error { error, .. } => {
                        AcpMcpOutcome::Error(serde_json::to_value(error).unwrap_or_default())
                    }
                    _ => return Err(self.unrecognized_response()),
                }
            }
        };
        match outcome {
            AcpMcpOutcome::Result(result) => self.complete_result(result),
            AcpMcpOutcome::Error(error) => {
                let size = serde_json::to_vec(&error)
                    .map_err(Error::into_internal_error)?
                    .len();
                if size > MCP_MAX_RESPONSE_BYTES {
                    return Err(failure(
                        format!(
                            "MCP-over-ACP error exceeded the {MCP_MAX_RESPONSE_BYTES}-byte limit"
                        ),
                        json!({"limit": MCP_MAX_RESPONSE_BYTES, "actual": size}),
                    ));
                }
                Err(failure(
                    format!(
                        "MCP server returned error {}: {}",
                        error.get("code").unwrap_or(&Value::Null),
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    ),
                    error,
                ))
            }
        }
    }

    /// MCP 2026-07-28 results say whether they are complete. A server that
    /// needs more input asks for it with `input_required`, which needs client
    /// capabilities this agent does not declare.
    fn complete_result(&self, result: Value) -> Result<Value, Error> {
        let size = serde_json::to_vec(&result)
            .map_err(Error::into_internal_error)?
            .len();
        if size > MCP_MAX_RESPONSE_BYTES {
            return Err(failure(
                format!("MCP-over-ACP response exceeded the {MCP_MAX_RESPONSE_BYTES}-byte limit"),
                json!({"limit": MCP_MAX_RESPONSE_BYTES, "actual": size}),
            ));
        }
        match result.get("resultType") {
            None => Ok(result),
            Some(Value::String(kind)) if kind == "complete" => Ok(result),
            Some(Value::String(kind)) if kind == "input_required" => Err(failure(
                "MCP server asked for more input, which dsh-acp does not support".to_string(),
                result,
            )),
            Some(_) => Err(failure(
                "MCP server returned an unrecognized resultType".to_string(),
                result,
            )),
        }
    }

    /// An outer ACP error: the client could not route or run the request, for
    /// example because the `serverId` is unknown (`-33001`). It also covers a
    /// client that cancels the request itself.
    fn binding_failure(&self, error: Error) -> Error {
        failure(
            format!("MCP-over-ACP client failed the request: {}", error.message),
            json!({"acpError": {
                "code": i32::from(error.code),
                "message": error.message,
                "data": error.data
            }}),
        )
    }

    fn unrecognized_response(&self) -> Error {
        let message = "MCP-over-ACP client sent an unrecognized response".to_string();
        failure(message.clone(), Value::String(message))
    }
}

/// A failed MCP request. The code is always internal error, whatever code the
/// client or MCP server used: theirs mean nothing to this ACP connection, and
/// must not end a turn as cancelled or read as a request to authenticate.
fn failure(message: String, data: Value) -> Error {
    let mut error = Error::internal_error().data(data);
    error.message = message;
    error
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
    if tools.len() > MCP_MAX_TOOLS_PER_PAGE {
        return Err(Error::internal_error().data(format!(
            "MCP tools/list page exceeded the {MCP_MAX_TOOLS_PER_PAGE}-tool limit"
        )));
    }
    for tool in tools {
        let size = serde_json::to_vec(tool)
            .map_err(Error::into_internal_error)?
            .len();
        if size > MCP_MAX_TOOL_METADATA_BYTES {
            return Err(Error::internal_error().data(format!(
                "MCP tool metadata exceeded the {MCP_MAX_TOOL_METADATA_BYTES}-byte limit"
            )));
        }
    }
    let tools: Vec<McpToolDescription> = tools
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
    if next_cursor
        .as_ref()
        .is_some_and(|cursor| cursor.len() > MCP_MAX_CURSOR_BYTES)
    {
        return Err(Error::internal_error().data(format!(
            "MCP pagination cursor exceeded the {MCP_MAX_CURSOR_BYTES}-byte limit"
        )));
    }
    Ok((tools, next_cursor))
}

struct StdioTransport {
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    child: Child,
    next_id: u64,
}

enum BoundedLine {
    Line(Vec<u8>),
    Eof,
    TooLarge,
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> std::io::Result<BoundedLine> {
    let mut line = Vec::with_capacity(limit.min(8192));
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if line.is_empty() {
                BoundedLine::Eof
            } else {
                BoundedLine::Line(line)
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > limit {
            reader.consume(take);
            return Ok(BoundedLine::TooLarge);
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(BoundedLine::Line(line));
        }
    }
}

/// Environment variables a stdio MCP server may inherit from the agent.
///
/// MCP servers are third-party programs. They get only the values needed to
/// start and run normally; credentials and toolchain injection variables must
/// be named explicitly in the server's `env` configuration.
const MCP_INHERITED_ENV: &[&str] = &[
    // Unix process basics.
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TZ",
    "TERM",
    "COLORTERM",
    // Common locale settings.
    "LANG",
    "LANGUAGE",
    "LC_ALL",
    "LC_ADDRESS",
    "LC_COLLATE",
    "LC_CTYPE",
    "LC_IDENTIFICATION",
    "LC_MEASUREMENT",
    "LC_MESSAGES",
    "LC_MONETARY",
    "LC_NAME",
    "LC_NUMERIC",
    "LC_PAPER",
    "LC_TELEPHONE",
    "LC_TIME",
    // Windows process basics. Environment names are case-insensitive there.
    "ALLUSERSPROFILE",
    "APPDATA",
    "COMMONPROGRAMFILES",
    "COMMONPROGRAMFILES(X86)",
    "COMMONPROGRAMW6432",
    "COMSPEC",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "NUMBER_OF_PROCESSORS",
    "OS",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMW6432",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "WINDIR",
];

fn environment_names_match(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

/// The environment passed to a stdio MCP process. Explicit server variables
/// win over an inherited baseline value with the same name.
fn stdio_mcp_environment(
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
    configured: &[EnvVariable],
) -> Vec<(OsString, OsString)> {
    let mut environment = inherited
        .into_iter()
        .filter(|(name, _)| {
            name.to_str().is_some_and(|name| {
                MCP_INHERITED_ENV
                    .iter()
                    .any(|allowed| environment_names_match(name, allowed))
            })
        })
        .filter(|(name, _)| {
            !name.to_str().is_some_and(|name| {
                configured
                    .iter()
                    .any(|variable| environment_names_match(name, &variable.name))
            })
        })
        .collect::<Vec<_>>();
    environment.extend(configured.iter().map(|variable| {
        (
            OsString::from(&variable.name),
            OsString::from(&variable.value),
        )
    }));
    environment
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
        process.env_clear();
        for (name, value) in stdio_mcp_environment(std::env::vars_os(), env) {
            process.env(name, value);
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

        loop {
            let line = tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
                result = read_bounded_line(&mut self.stdout, MCP_MAX_LINE_BYTES) => {
                    result.map_err(Error::into_internal_error)?
                }
            };
            let line = match line {
                BoundedLine::Line(line) => line,
                BoundedLine::Eof => {
                    return Err(Error::internal_error().data(format!(
                        "MCP server exited with status {:?}",
                        self.child
                            .try_wait()
                            .ok()
                            .flatten()
                            .and_then(|status| status.code())
                    )));
                }
                BoundedLine::TooLarge => {
                    let _ = self.child.kill().await;
                    return Err(Error::internal_error().data(format!(
                        "MCP stdio response exceeded the {MCP_MAX_LINE_BYTES}-byte line limit"
                    )));
                }
            };
            let Ok(value) = serde_json::from_slice::<Value>(&line) else {
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
    fn new(url: &str, headers: &[HttpHeader], allow_insecure_http: bool) -> Result<Self, Error> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| Error::invalid_params().data(error.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::invalid_params().data("MCP URL must use http or https"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Error::invalid_params()
                .data("MCP URL must not contain credentials; configure authentication headers"));
        }
        if url.scheme() == "http" && !is_loopback_url(&url) && !allow_insecure_http {
            return Err(Error::invalid_params().data(
                "plaintext HTTP MCP URLs are only allowed on loopback; use HTTPS or set DSH_ACP_ALLOW_INSECURE_MCP_HTTP=1",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(180))
            .redirect(reqwest::redirect::Policy::none())
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
            response = request.send() => response.map_err(http_error)?,
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
            let bytes = match read_http_prefix(response, ERROR_BODY_BYTES, cancellation).await {
                Ok(bytes) => bytes,
                Err(_) if cancellation.is_cancelled() => {
                    return Err(Error::request_cancelled());
                }
                Err(_) => Vec::new(),
            };
            let text = String::from_utf8_lossy(&bytes);
            return Err(Error::internal_error().data(json!({
                "status": status.as_u16(),
                "message": truncate_body(&text)
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
            let bytes = read_http_body(
                response,
                MCP_MAX_RESPONSE_BYTES,
                cancellation,
                "MCP HTTP response",
            )
            .await?;
            serde_json::from_slice(&bytes).map_err(Error::into_internal_error)
        }
    }
}

fn is_loopback_url(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

/// A failed HTTP exchange with an MCP server, with its causes, which
/// reqwest's own message omits ("error sending request" without "Connection
/// refused"). The URL is left out: it can carry credentials, and this detail
/// reaches the model when a tool call fails.
fn http_error(error: reqwest::Error) -> Error {
    let error = error.without_url();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !message.contains(&cause_text) {
            message.push_str(": ");
            message.push_str(&cause_text);
        }
        source = cause.source();
    }
    Error::internal_error().data(message)
}

async fn read_http_body(
    response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
    label: &str,
) -> Result<Vec<u8>, Error> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(
            Error::internal_error().data(format!("{label} exceeded the {limit}-byte limit"))
        );
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(limit.min(32 * 1024));
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Ok(bytes);
        };
        let chunk = chunk.map_err(http_error)?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(
                Error::internal_error().data(format!("{label} exceeded the {limit}-byte limit"))
            );
        }
        bytes.extend_from_slice(&chunk);
    }
}

async fn read_http_prefix(
    response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, Error> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(limit);
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Ok(bytes);
        };
        let chunk = chunk.map_err(http_error)?;
        let remaining = limit.saturating_sub(bytes.len());
        let take = remaining.min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
        if take < chunk.len() || bytes.len() == limit {
            return Ok(bytes);
        }
    }
}

/// The most of an error response body kept for the model. A proxy's error
/// page can be large, and only its start says what went wrong.
const ERROR_BODY_BYTES: usize = 2048;

fn truncate_body(body: &str) -> String {
    if body.len() <= ERROR_BODY_BYTES {
        return body.to_string();
    }
    let mut boundary = ERROR_BODY_BYTES;
    while !body.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}[...truncated...]", &body[..boundary])
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
        let chunk = chunk.map_err(http_error)?;
        if buffer.len().saturating_add(chunk.len()) > MCP_MAX_LINE_BYTES {
            return Err(Error::internal_error().data(format!(
                "MCP SSE input exceeded the {MCP_MAX_LINE_BYTES}-byte line limit"
            )));
        }
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
                let payload = payload.trim_start();
                let added = payload.len() + usize::from(!data.is_empty());
                if data.len().saturating_add(added) > MCP_MAX_RESPONSE_BYTES {
                    return Err(Error::internal_error().data(format!(
                        "MCP SSE event exceeded the {MCP_MAX_RESPONSE_BYTES}-byte limit"
                    )));
                }
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(payload);
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

    #[tokio::test]
    async fn http_errors_keep_their_cause_and_drop_the_url() {
        // Nothing listens on the discard port, so the connection is refused.
        let error = reqwest::get("http://user:secret@127.0.0.1:9/mcp?token=secret")
            .await
            .unwrap_err();
        let reason = crate::tools::error_reason(&http_error(error));
        assert!(!reason.contains("secret"), "{reason}");
        assert_ne!(reason, "error sending request", "{reason}");
    }

    #[test]
    fn remote_mcp_http_requires_explicit_plaintext_opt_in() {
        let remote = "http://mcp.example.test/rpc";
        assert!(HttpTransport::new(remote, &[], false).is_err());
        assert!(HttpTransport::new(remote, &[], true).is_ok());
        assert!(HttpTransport::new("http://127.0.0.1:9000/rpc", &[], false).is_ok());
        assert!(HttpTransport::new("http://[::1]:9000/rpc", &[], false).is_ok());
        assert!(HttpTransport::new("https://mcp.example.test/rpc", &[], false).is_ok());
        assert!(
            HttpTransport::new("https://user:secret@mcp.example.test/rpc", &[], false).is_err()
        );
    }

    #[tokio::test]
    async fn authenticated_mcp_requests_do_not_follow_redirects() {
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_url = format!("http://{}", target.local_addr().unwrap());
        let redirect = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_url = format!("http://{}", redirect.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = redirect.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let headers = [HttpHeader::new("Authorization", "Bearer secret")];
        let mut transport = HttpTransport::new(&redirect_url, &headers, false).unwrap();
        let error = transport
            .post(
                json!({"jsonrpc": "2.0", "method": "notifications/test"}),
                &CancellationToken::new(),
            )
            .await
            .expect_err("a redirect response must not be followed");
        assert_eq!(error.data.as_ref().unwrap()["status"], 302);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), target.accept())
                .await
                .is_err(),
            "the redirect target received the authenticated request"
        );
        server.await.unwrap();
    }

    #[test]
    fn error_bodies_are_cut_on_a_char_boundary() {
        assert_eq!(truncate_body("short"), "short");
        let body = "é".repeat(ERROR_BODY_BYTES);
        let truncated = truncate_body(&body);
        assert!(truncated.ends_with("[...truncated...]"));
        assert!(truncated.len() <= ERROR_BODY_BYTES + "[...truncated...]".len());
    }

    #[tokio::test]
    async fn stdio_lines_are_read_without_crossing_the_limit() {
        let input = b"short\nthis line is too long\n";
        let mut reader = BufReader::new(input.as_slice());
        let BoundedLine::Line(line) = read_bounded_line(&mut reader, 32).await.unwrap() else {
            panic!("the short line should be readable");
        };
        assert_eq!(line, b"short\n");
        assert!(matches!(
            read_bounded_line(&mut reader, 8).await.unwrap(),
            BoundedLine::TooLarge
        ));
    }

    #[test]
    fn mcp_tool_metadata_and_cursors_are_bounded() {
        let too_many = json!({
            "tools": (0..MCP_MAX_TOOLS_PER_PAGE + 1)
                .map(|index| json!({"name": format!("tool-{index}")}))
                .collect::<Vec<_>>()
        });
        assert!(parse_tool_list(&too_many).is_err());

        let oversized = json!({
            "tools": [{
                "name": "large",
                "description": "x".repeat(MCP_MAX_TOOL_METADATA_BYTES)
            }]
        });
        assert!(parse_tool_list(&oversized).is_err());

        let long_cursor = json!({
            "tools": [],
            "nextCursor": "x".repeat(MCP_MAX_CURSOR_BYTES + 1)
        });
        assert!(parse_tool_list(&long_cursor).is_err());
    }

    #[tokio::test]
    async fn oversized_http_mcp_bodies_fail_cleanly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            socket.write_all(&[b'x'; 64]).await.unwrap();
        });
        let response = reqwest::get(format!("http://{address}")).await.unwrap();
        let error = read_http_body(response, 16, &CancellationToken::new(), "test MCP response")
            .await
            .unwrap_err();
        assert!(
            crate::tools::error_reason(&error).contains("16-byte limit"),
            "{error:?}"
        );
        server.await.unwrap();
    }

    #[test]
    fn stdio_mcp_environment_is_allowlisted_and_explicit_values_win() {
        let inherited = [
            (OsString::from("PATH"), OsString::from("/usr/bin")),
            (
                OsString::from("DEEPSEEK_API_KEY"),
                OsString::from("ambient-provider-secret"),
            ),
            (
                OsString::from("AWS_SECRET_ACCESS_KEY"),
                OsString::from("ambient-cloud-secret"),
            ),
            (
                OsString::from("EXPLICIT_MCP_TOKEN"),
                OsString::from("ambient-value"),
            ),
        ];
        let configured = [EnvVariable::new("EXPLICIT_MCP_TOKEN", "configured-value")];

        let environment = stdio_mcp_environment(inherited, &configured);

        assert!(environment.contains(&(OsString::from("PATH"), OsString::from("/usr/bin"))));
        assert!(
            !environment
                .iter()
                .any(|(name, _)| name == "DEEPSEEK_API_KEY")
        );
        assert!(
            !environment
                .iter()
                .any(|(name, _)| name == "AWS_SECRET_ACCESS_KEY")
        );
        assert_eq!(
            environment
                .iter()
                .filter(|(name, _)| name == "EXPLICIT_MCP_TOKEN")
                .map(|(_, value)| value.as_os_str())
                .collect::<Vec<_>>(),
            [OsString::from("configured-value").as_os_str()]
        );
    }

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

    #[tokio::test]
    async fn a_repeated_acp_server_id_is_rejected_before_anything_starts() {
        let servers = [
            McpServer::Acp(v1::McpServerAcp::new("one", "tools:1")),
            McpServer::Acp(v1::McpServerAcp::new("two", "tools:1")),
        ];
        let error = McpSession::connect(
            &servers,
            None,
            Duration::from_secs(1),
            false,
            &CancellationToken::new(),
        )
        .await
        .expect_err("a repeated serverId should be rejected");
        assert_eq!(error.code, agent_client_protocol::ErrorCode::InvalidParams);
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
                None,
                Duration::from_millis(300),
                false,
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
            None,
            Duration::from_secs(10),
            false,
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
    async fn repeated_mcp_tool_cursors_fail_instead_of_looping() {
        let script = r#"
            read -r line
            echo '{"jsonrpc":"2.0","id":1,"result":{}}'
            read -r line
            read -r line
            echo '{"jsonrpc":"2.0","id":2,"result":{"tools":[],"nextCursor":"again"}}'
            read -r line
            echo '{"jsonrpc":"2.0","id":3,"result":{"tools":[],"nextCursor":"again"}}'
            while read -r line; do :; done
        "#;
        let error = McpSession::connect(
            &[shell_server("cursor", script, &[])],
            None,
            Duration::from_secs(10),
            false,
            &CancellationToken::new(),
        )
        .await
        .expect_err("a repeated cursor should fail setup");
        assert!(
            crate::tools::error_reason(&error).contains("repeated pagination cursor"),
            "{error:?}"
        );
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
                None,
                Duration::from_millis(500),
                false,
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
