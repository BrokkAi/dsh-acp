use std::{
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use agent_client_protocol::{
    Client, ConnectionTo,
    schema::v1::{
        ClientCapabilities, CreateTerminalRequest, EnvVariable, KillTerminalRequest,
        ReleaseTerminalRequest, SessionNotification, SessionUpdate, Terminal, TerminalId,
        TerminalOutputRequest, ToolCallContent, ToolCallStatus, ToolCallUpdate,
        ToolCallUpdateFields, WaitForTerminalExitRequest, WriteTextFileRequest,
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::{
    config::{Config, PermissionMode},
    model::{
        ChatMessage, MessageContent, PersistedDiff, PersistedTerminal, PersistedToolOutcome, Role,
        SessionRecord, ToolCall, ToolOutcomeStatus,
    },
    session::SessionStore,
};

#[derive(Clone)]
pub struct ToolAccess {
    pub connection: Option<ConnectionTo<Client>>,
    pub client: ClientCapabilities,
    pub config: Config,
}

/// The ACP v1 tool call a command runs for. When the command runs in the
/// client's terminal, `run_command` puts that terminal in this tool call, so
/// the client shows the output there rather than in a tool call of its own,
/// and records the terminal so the final update can keep showing it.
pub struct TerminalToolCall {
    id: String,
    terminal_id: std::sync::OnceLock<TerminalId>,
}

impl TerminalToolCall {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            terminal_id: std::sync::OnceLock::new(),
        }
    }

    /// The client terminal the command ran in, if the client created one. It
    /// is set even when the command then failed or timed out.
    pub fn terminal_id(&self) -> Option<&TerminalId> {
        self.terminal_id.get()
    }
}

#[derive(Debug, Clone)]
pub struct ToolResult {
    pub text: String,
    pub raw: Value,
}

pub fn persisted_tool_outcome(
    call: &ToolCall,
    result: &ToolResult,
    status: ToolOutcomeStatus,
) -> PersistedToolOutcome {
    PersistedToolOutcome {
        status,
        text: result.text.clone(),
        raw_output: result.raw.clone(),
        structured_content: structured_content(&result.raw),
        diff: persisted_diff(&call.function.name, &result.raw),
        terminal: persisted_terminal(&call.function.name, &result.raw),
    }
}

pub fn result_status(result: &ToolResult) -> ToolOutcomeStatus {
    if result.raw.get("error").is_some()
        || result
            .raw
            .get("isError")
            .and_then(Value::as_bool)
            .is_some_and(|is_error| is_error)
    {
        ToolOutcomeStatus::Failed
    } else {
        ToolOutcomeStatus::Completed
    }
}

pub fn structured_content(raw: &Value) -> Vec<Value> {
    raw.get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| {
                    matches!(
                        item.get("type").and_then(Value::as_str),
                        Some("image" | "audio" | "resource" | "resource_link")
                    )
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

pub fn persisted_diff(tool: &str, raw: &Value) -> Option<PersistedDiff> {
    if !matches!(tool, "write_file" | "edit_file") {
        return None;
    }
    Some(PersistedDiff {
        path: raw.get("path")?.as_str()?.to_string(),
        old_text: raw
            .get("_old_text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        new_text: raw.get("_new_text")?.as_str()?.to_string(),
    })
}

pub fn persisted_terminal(tool: &str, raw: &Value) -> Option<PersistedTerminal> {
    if tool != "run_command" {
        return None;
    }
    let terminal_id = raw.get("_terminal_id")?.as_str()?.to_string();
    let stdout = raw
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stderr = raw
        .get("stderr")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut output = String::with_capacity(stdout.len() + stderr.len() + 1);
    output.push_str(stdout);
    if !stderr.is_empty() && !stdout.is_empty() && !stdout.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(stderr);
    let failed = raw.get("error").is_some();
    let exit_code = raw
        .get("exit_code")
        .and_then(Value::as_u64)
        .and_then(|code| u32::try_from(code).ok());
    let signal = raw
        .get("signal")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some(PersistedTerminal {
        terminal_id,
        command: command_display(raw),
        cwd: raw
            .get("cwd")
            .and_then(Value::as_str)
            .map(std::path::PathBuf::from)
            .unwrap_or_default(),
        output,
        exit_code,
        signal,
        exited: !failed && (exit_code.is_some() || raw.get("exit_code").is_some()),
    })
}

/// A command and its arguments as a shell would read them. A permission
/// request shows this, so `sh -c 'ls && rm x'` must not read as `sh -c ls`
/// followed by a second command.
pub fn command_display(raw: &Value) -> String {
    let command = raw
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("command");
    std::iter::once(command)
        .chain(
            raw.get("args")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str),
        )
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(word: &str) -> String {
    let plain = !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./=:,@%+".contains(c));
    if plain {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', r"'\''"))
    }
}

pub fn git_patch(diff: &PersistedDiff) -> String {
    let path = diff.path.as_str();
    let new = diff.new_text.as_str();
    match &diff.old_text {
        None => {
            let count = line_count(new);
            format!(
                "diff --git {path} {path}\n--- /dev/null\n+++ {path}\n@@ -0,0 {} @@\n{}",
                hunk_range(1, count),
                prefixed_lines("+", new)
            )
        }
        Some(old) => {
            let old_count = line_count(old);
            let new_count = line_count(new);
            format!(
                "diff --git {path} {path}\n--- {path}\n+++ {path}\n@@ -{} +{} @@\n{}{}",
                hunk_range(1, old_count),
                hunk_range(1, new_count),
                prefixed_lines("-", old),
                prefixed_lines("+", new)
            )
        }
    }
}

fn hunk_range(start: usize, count: usize) -> String {
    if count == 1 {
        start.to_string()
    } else {
        format!("{start},{count}")
    }
}

fn line_count(value: &str) -> usize {
    value.lines().count()
}

fn prefixed_lines(prefix: &str, value: &str) -> String {
    let mut output = String::new();
    for line in value.lines() {
        output.push_str(prefix);
        output.push_str(line);
        output.push('\n');
    }
    output
}

pub fn built_in_definitions(config: &Config) -> Vec<Value> {
    let mut definitions = vec![
        function_tool(
            "update_plan",
            "Replace the ACP-visible execution plan with a complete ordered list of tasks and statuses.",
            json!({
                "type": "object",
                "properties": {
                    "entries": {
                        "type": "array",
                        "maxItems": 50,
                        "items": {
                            "type": "object",
                            "properties": {
                                "content": {"type": "string"},
                                "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                                "status": {"type": "string", "enum": ["pending", "in_progress", "completed"]}
                            },
                            "required": ["content", "priority", "status"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["entries"],
                "additionalProperties": false
            }),
        ),
        function_tool(
            "read_file",
            "Read a UTF-8 text file from the workspace. Supports line ranges.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path or path relative to the session cwd."},
                    "start_line": {"type": "integer", "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 10000}
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        ),
        function_tool(
            "list_directory",
            "List files and directories under a workspace path.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory relative to cwd. Defaults to cwd."},
                    "depth": {"type": "integer", "minimum": 1, "maximum": 8, "default": 2},
                    "include_hidden": {"type": "boolean", "default": false},
                    "max_entries": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 1000}
                },
                "additionalProperties": false
            }),
        ),
        function_tool(
            "search_files",
            "Search text files by regular expression, optionally filtering paths by regex.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Rust regular expression to find."},
                    "path": {"type": "string", "description": "Directory relative to cwd. Defaults to cwd."},
                    "path_filter": {"type": "string", "description": "Optional regex applied to relative file paths."},
                    "case_sensitive": {"type": "boolean", "default": true},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 2000, "default": 200}
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        ),
        function_tool(
            "write_file",
            "Create or completely overwrite a UTF-8 text file. Requires user permission.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                    "create_directories": {"type": "boolean", "default": true}
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        ),
        function_tool(
            "edit_file",
            "Replace exact text in a file. Fails when old_text is absent or ambiguous unless replace_all is true. Requires user permission.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "old_text": {"type": "string"},
                    "new_text": {"type": "string"},
                    "replace_all": {"type": "boolean", "default": false}
                },
                "required": ["path", "old_text", "new_text"],
                "additionalProperties": false
            }),
        ),
        function_tool(
            "run_command",
            "Run a command without a shell and return stdout/stderr and exit status. Requires user permission.",
            json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Executable name or absolute path."},
                    "args": {"type": "array", "items": {"type": "string"}, "default": []},
                    "cwd": {"type": "string", "description": "Optional workspace-relative working directory."},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 1800, "default": 120}
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        ),
    ];
    if !config.disable_web {
        definitions.push(function_tool(
            "web_fetch",
            "Fetch a public HTTP(S) URL and return bounded, readable text. Use for documentation and current web content.",
            json!({
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "max_bytes": {"type": "integer", "minimum": 1024, "maximum": 1048576, "default": 262144}
                },
                "required": ["url"],
                "additionalProperties": false
            }),
        ));
    }
    definitions
}

pub fn function_tool(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters
        }
    })
}

pub fn ask_user_definition() -> Value {
    function_tool(
        "ask_user",
        "Ask the user a focused question when a decision or missing detail cannot be resolved from the workspace. Provide a URL instead of relying on a form when sensitive or externally hosted interaction is required.",
        json!({
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "The question to show the user."},
                "url": {"type": "string", "description": "Optional HTTPS URL for an out-of-band interaction."},
                "elicitation_id": {"type": "string", "description": "Optional identifier for a URL interaction; omit it to have one generated. An ID still in use by a URL interaction that was not reported complete is refused."}
            },
            "required": ["message"],
            "additionalProperties": false
        }),
    )
}

/// Offered with [`ask_user_definition`] when the Client supports URL
/// elicitation. The harness cannot observe an out-of-band interaction finish,
/// so the model reports it once it has checked.
pub fn complete_url_elicitation_definition() -> Value {
    function_tool(
        "complete_url_elicitation",
        "Tell the client that a URL interaction started with ask_user has finished, such as an authorization the user completed in their browser. Call it only after verifying that it finished, for example by retrying the operation that needed it, and only with an elicitation ID that ask_user reported as accepted.",
        json!({
            "type": "object",
            "properties": {
                "elicitation_id": {"type": "string", "description": "The elicitation ID from the accepted ask_user result."}
            },
            "required": ["elicitation_id"],
            "additionalProperties": false
        }),
    )
}

/// The result of an accepted URL elicitation. Consent to open the URL is not
/// completion, so the model is told so, along with the ID to pass to
/// `complete_url_elicitation` once it has verified the interaction finished.
pub fn url_elicitation_accepted(elicitation_id: &str) -> ToolResult {
    ToolResult {
        text: format!(
            "The user consented to open the URL. External completion was not verified. Elicitation ID: {elicitation_id}. Once you have confirmed the interaction finished, for example by retrying the operation that needed it, call complete_url_elicitation with this ID."
        ),
        raw: json!({"action": "accept", "elicitation_id": elicitation_id}),
    }
}

/// Why `error` failed. A string `data` is the reason. A message that only
/// names the JSON-RPC error does not tell the model what to change, so under
/// one, other `data` is shown as compact JSON; a message written for this
/// failure already explains it and is kept.
pub fn error_reason(error: &agent_client_protocol::Error) -> String {
    let generic = error.message == agent_client_protocol::Error::from(error.code).message;
    match &error.data {
        Some(Value::String(reason)) if !reason.trim().is_empty() => reason.clone(),
        Some(data) if generic && !data.is_null() && !data.is_string() => data.to_string(),
        _ => error.message.clone(),
    }
}

/// A failed tool result for `error`, with the reason from [`error_reason`].
pub fn error_result(context: &str, error: &agent_client_protocol::Error) -> ToolResult {
    let reason = error_reason(error);
    ToolResult {
        text: format!("{context}: {reason}"),
        raw: json!({"error": reason}),
    }
}

/// The failed result of an `ask_user` request that was refused before it was
/// sent, or that the Client answered with an error. A Client that cancels the
/// request itself answers `-32800`, which names no reason; that fails only
/// this call, because only the turn's own cancellation ends the turn.
pub fn ask_user_failed(error: &agent_client_protocol::Error) -> ToolResult {
    if error.code == agent_client_protocol::ErrorCode::RequestCancelled {
        let reason = "the client cancelled the request";
        return ToolResult {
            text: format!("Unable to request user input: {reason}"),
            raw: json!({"error": reason}),
        };
    }
    error_result("Unable to request user input", error)
}

/// The failed result of a tool call whose permission request the Client
/// answered with an error. No permission was granted, so the tool does not
/// run, as for a denial. A Client that cancels the request itself answers
/// `-32800`, which names no reason; that fails only this call, because only
/// the turn's own cancellation ends the turn.
pub fn permission_request_failed(error: &agent_client_protocol::Error) -> ToolResult {
    let reason = if error.code == agent_client_protocol::ErrorCode::RequestCancelled {
        "the client cancelled the permission request".to_string()
    } else {
        error_reason(error)
    };
    ToolResult {
        text: format!("Permission was not granted, so the tool call did not run: {reason}"),
        raw: json!({"denied": true, "error": reason}),
    }
}

/// Run `complete_url_elicitation`: take the session's accepted URL
/// elicitation with the given ID and `send` its `elicitation/complete`. An ID
/// that is unknown, already completed, or another session's fails the call
/// without sending anything.
pub async fn complete_url_elicitation(
    store: &SessionStore,
    session_id: &str,
    arguments: &Value,
    send: impl FnOnce(String) -> Result<(), agent_client_protocol::Error>,
) -> ToolResult {
    let Some(elicitation_id) = arguments.get("elicitation_id").and_then(Value::as_str) else {
        return ToolResult {
            text: "complete_url_elicitation requires an elicitation_id.".to_string(),
            raw: json!({"error": "missing elicitation_id"}),
        };
    };
    if !store
        .complete_url_elicitation(session_id, elicitation_id)
        .await
    {
        return ToolResult {
            text: format!(
                "No accepted URL elicitation {elicitation_id} is awaiting completion in this session. Nothing was reported to the client."
            ),
            raw: json!({
                "error": "no pending URL elicitation",
                "elicitation_id": elicitation_id
            }),
        };
    }
    if let Err(error) = send(elicitation_id.to_string()) {
        return error_result("Unable to report the URL elicitation complete", &error);
    }
    ToolResult {
        text: format!("Reported URL elicitation {elicitation_id} complete to the client."),
        raw: json!({"elicitation_id": elicitation_id, "completed": true}),
    }
}

pub fn permission_required(name: &str) -> bool {
    matches!(
        name,
        "write_file" | "edit_file" | "run_command" | "mcp__write" | "mcp__delete"
    )
}

/// What the model is told while a session is in plan mode. Without it the
/// model is not told it is planning, tries to make the change, and the user is
/// asked to approve edits that plan mode then refuses.
pub const PLAN_MODE_INSTRUCTIONS: &str = "The session is in plan mode. You can read, list, and search files, fetch web pages, and use read-only MCP tools. You cannot write or edit files, run commands, or use other MCP tools. Investigate as needed, then reply with a concrete plan for the user to review. Do not say you made changes.";

/// The reply to a tool call that plan mode refuses before any permission is
/// requested.
pub const PLAN_MODE_REFUSAL: &str = "The session is in plan mode. Switch to agent mode before modifying files, running commands, or using MCP tools that are not read-only.";

/// Whether plan mode refuses this tool: built-in tools that change the
/// workspace, and every MCP tool, whose mutation safety cannot be determined.
pub fn refused_in_plan_mode(name: &str, read_only_mcp: bool) -> bool {
    matches!(name, "write_file" | "edit_file" | "run_command")
        || (name.starts_with("mcp__") && !read_only_mcp)
}

/// Restrict a model request to plan mode: drop the tools plan mode refuses and
/// add [`PLAN_MODE_INSTRUCTIONS`] to the system prompt.
pub fn restrict_to_plan_mode(
    tools: &mut Vec<Value>,
    context: &mut Vec<ChatMessage>,
    read_only_mcp_names: &[String],
) {
    tools.retain(|tool| {
        !tool["function"]["name"].as_str().is_some_and(|name| {
            refused_in_plan_mode(
                name,
                read_only_mcp_names.iter().any(|allowed| allowed == name),
            )
        })
    });
    match context.first_mut() {
        Some(ChatMessage {
            role: Role::System,
            content: Some(MessageContent::Text(prompt)),
            ..
        }) => {
            prompt.push_str("\n\n");
            prompt.push_str(PLAN_MODE_INSTRUCTIONS);
        }
        _ => context.insert(0, ChatMessage::system(PLAN_MODE_INSTRUCTIONS)),
    }
}

/// Run a built-in tool. `terminal_call` is the ACP v1 tool call that shows a
/// client terminal; it is `None` where the client runs no terminals (ACP v2).
pub async fn execute(
    access: &ToolAccess,
    session: &SessionRecord,
    name: &str,
    arguments: Value,
    terminal_call: Option<&TerminalToolCall>,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    if session.config.mode == "plan" && refused_in_plan_mode(name, false) {
        return Err(agent_client_protocol::Error::invalid_request().data(PLAN_MODE_REFUSAL));
    }

    match name {
        "read_file" => read_file(access, session, &arguments, cancellation).await,
        "list_directory" => list_directory(session, &arguments, cancellation),
        "search_files" => search_files(session, &arguments, cancellation),
        "write_file" => write_file(access, session, &arguments, cancellation).await,
        "edit_file" => edit_file(access, session, &arguments, cancellation).await,
        "run_command" => {
            run_command(access, session, &arguments, terminal_call, cancellation).await
        }
        "web_fetch" => web_fetch(access, session, &arguments, cancellation).await,
        _ => Err(agent_client_protocol::Error::invalid_params()
            .data(format!("unknown built-in tool: {name}"))),
    }
}

async fn read_file(
    access: &ToolAccess,
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let path = resolve_existing_path(session, required_string(arguments, "path")?)?;
    let start = arguments
        .get("start_line")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .clamp(1, u32::MAX as u64) as usize;
    let limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(4000)
        .min(10_000) as usize;
    let client_range = access.client.fs.read_text_file && access.connection.is_some();
    let content = if client_range {
        let connection = access
            .connection
            .as_ref()
            .expect("client range request requires an ACP v1 connection");
        let request = agent_client_protocol::schema::v1::ReadTextFileRequest::new(
            session.id.clone(),
            path.clone(),
        )
        .line(Some(start as u32))
        .limit(Some(limit as u32));
        tokio::select! {
            _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
            response = connection.send_request(request).block_task() => {
                response?.content
            }
        }
    } else {
        tokio::select! {
            _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
            result = tokio::fs::read_to_string(&path) => result.map_err(|error| {
                agent_client_protocol::Error::resource_not_found(Some(format!("file://{}", path.display())))
                    .data(error.to_string())
            })?,
        }
    };

    let selected = if client_range {
        content
    } else {
        content
            .lines()
            .skip(start.saturating_sub(1))
            .take(limit)
            .map(|line| format!("{line}\n"))
            .collect::<String>()
    };
    let selected = truncate(&selected, access.config.tool_output_bytes);
    Ok(ToolResult {
        text: selected.clone(),
        raw: json!({
            "path": path,
            "content": selected,
            "start_line": start,
            "limit": limit
        }),
    })
}

fn list_directory(
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let root = match arguments.get("path").and_then(Value::as_str) {
        Some(path) => resolve_existing_path(session, path)?,
        None => session.cwd.clone(),
    };
    let depth = arguments
        .get("depth")
        .and_then(Value::as_u64)
        .unwrap_or(2)
        .clamp(1, 8) as usize;
    let include_hidden = arguments
        .get("include_hidden")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let max_entries = arguments
        .get("max_entries")
        .and_then(Value::as_u64)
        .unwrap_or(1000)
        .clamp(1, 5000) as usize;

    let mut entries = Vec::new();
    for entry in WalkDir::new(&root)
        .min_depth(1)
        .max_depth(depth)
        .follow_links(false)
        .into_iter()
    {
        if cancellation.is_cancelled() {
            return Err(agent_client_protocol::Error::request_cancelled());
        }
        let Ok(entry) = entry else {
            continue;
        };
        let relative = entry.path().strip_prefix(&root).unwrap_or(entry.path());
        if !include_hidden && is_hidden_path(relative) {
            continue;
        }
        if !include_hidden && is_noisy_directory(relative) {
            continue;
        }
        let suffix = if entry.file_type().is_dir() { "/" } else { "" };
        entries.push(format!("{}{}", relative.display(), suffix));
        if entries.len() >= max_entries {
            break;
        }
    }
    entries.sort();
    let text = truncate(&entries.join("\n"), session_output_budget(session, 131_072));
    Ok(ToolResult {
        text: text.clone(),
        raw: json!({"root": root, "entries": text, "count": entries.len()}),
    })
}

fn search_files(
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let query = required_string(arguments, "query")?;
    let case_sensitive = arguments
        .get("case_sensitive")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let pattern = if case_sensitive {
        query.to_string()
    } else {
        format!("(?i:{query})")
    };
    let regex = regex::Regex::new(&pattern)
        .map_err(|error| agent_client_protocol::Error::invalid_params().data(error.to_string()))?;
    let path_filter = arguments
        .get("path_filter")
        .and_then(Value::as_str)
        .map(regex::Regex::new)
        .transpose()
        .map_err(|error| agent_client_protocol::Error::invalid_params().data(error.to_string()))?;
    let root = match arguments.get("path").and_then(Value::as_str) {
        Some(path) => resolve_existing_path(session, path)?,
        None => session.cwd.clone(),
    };
    let max_results = arguments
        .get("max_results")
        .and_then(Value::as_u64)
        .unwrap_or(200)
        .clamp(1, 2000) as usize;

    let mut matches = Vec::new();
    for entry in WalkDir::new(&root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            entry.path().strip_prefix(&root).map_or(true, |relative| {
                !is_noisy_directory(relative) && !is_hidden_path(relative)
            })
        })
    {
        if cancellation.is_cancelled() {
            return Err(agent_client_protocol::Error::request_cancelled());
        }
        let Ok(entry) = entry else {
            continue;
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if entry
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 16 * 1024 * 1024)
        {
            continue;
        }
        let relative = entry.path().strip_prefix(&root).unwrap_or(entry.path());
        if path_filter
            .as_ref()
            .is_some_and(|filter| !filter.is_match(&relative.to_string_lossy()))
        {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        if content.as_bytes().contains(&0) {
            continue;
        }
        for (line_index, line) in content.lines().enumerate() {
            if regex.is_match(line) {
                matches.push(format!(
                    "{}:{}:{}",
                    relative.display(),
                    line_index + 1,
                    truncate(line, 500)
                ));
                if matches.len() >= max_results {
                    break;
                }
            }
        }
        if matches.len() >= max_results {
            break;
        }
    }
    let text = truncate(&matches.join("\n"), 131_072);
    Ok(ToolResult {
        text: text.clone(),
        raw: json!({"root": root, "matches": text, "count": matches.len()}),
    })
}

async fn write_file(
    access: &ToolAccess,
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let path = resolve_writable_path(session, required_string(arguments, "path")?)?;
    let content = required_string(arguments, "content")?.to_string();
    let create_directories = arguments
        .get("create_directories")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if create_directories && let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(agent_client_protocol::Error::into_internal_error)?;
    }
    let old_text = tokio::fs::read_to_string(&path).await.ok();
    if access.client.fs.write_text_file
        && let Some(connection) = access.connection.as_ref()
    {
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
            response = connection
                .send_request(WriteTextFileRequest::new(
                    session.id.clone(),
                    path.clone(),
                    content.clone(),
                ))
                .block_task() => response?,
        };
        drop(response);
    } else {
        tokio::select! {
            _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
            result = tokio::fs::write(&path, &content) => result.map_err(agent_client_protocol::Error::into_internal_error)?,
        }
    }
    Ok(ToolResult {
        text: format!("Wrote {} bytes to {}", content.len(), path.display()),
        raw: json!({
            "path": path,
            "bytes": content.len(),
            "_old_text": old_text,
            "_new_text": content
        }),
    })
}

async fn edit_file(
    access: &ToolAccess,
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let path = resolve_existing_path(session, required_string(arguments, "path")?)?;
    let old_text = required_string(arguments, "old_text")?;
    let new_text = required_string(arguments, "new_text")?;
    let replace_all = arguments
        .get("replace_all")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let original = tokio::fs::read_to_string(&path)
        .await
        .map_err(agent_client_protocol::Error::into_internal_error)?;
    let (updated, occurrences) = replace_text(&original, old_text, new_text, replace_all)?;
    let write_args = json!({"path": path, "content": updated.clone(), "create_directories": false});
    let mut result = write_file(access, session, &write_args, cancellation).await?;
    result.text = format!(
        "Replaced {} occurrence(s) in {}",
        occurrences.min(if replace_all { occurrences } else { 1 }),
        path.display()
    );
    result.raw = json!({
        "path": path,
        "occurrences": occurrences,
        "replace_all": replace_all,
        "_old_text": original,
        "_new_text": updated
    });
    Ok(result)
}

/// The text `edit_file` writes: `old_text` must occur exactly once unless
/// `replace_all` is set. Returns the new text and how often `old_text` occurs.
fn replace_text(
    original: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
) -> Result<(String, usize), agent_client_protocol::Error> {
    let occurrences = original.matches(old_text).count();
    if occurrences == 0 {
        return Err(agent_client_protocol::Error::invalid_params()
            .data("old_text was not found; read the current file before editing it"));
    }
    if occurrences > 1 && !replace_all {
        return Err(agent_client_protocol::Error::invalid_params().data(format!(
            "old_text occurs {occurrences} times; provide more context or set replace_all=true"
        )));
    }
    let updated = if replace_all {
        original.replace(old_text, new_text)
    } else {
        original.replacen(old_text, new_text, 1)
    };
    Ok((updated, occurrences))
}

/// The change a `write_file` or `edit_file` call would make, worked out
/// without writing anything, so the tool call and its permission request can
/// show it before the user approves. The path is resolved and confined as the
/// tool resolves it, and only the file the tool itself would read is read.
///
/// `None` when the tool would fail (an edit of a missing file, `old_text` not
/// found or ambiguous, a path outside the workspace roots) or the current file
/// is not UTF-8 text. The tool then reports its own error when it runs.
pub async fn preview_diff(
    session: &SessionRecord,
    tool: &str,
    arguments: &Value,
) -> Option<PersistedDiff> {
    match tool {
        "edit_file" => {
            let path =
                resolve_existing_path(session, required_string(arguments, "path").ok()?).ok()?;
            let old_text = required_string(arguments, "old_text").ok()?;
            let new_text = required_string(arguments, "new_text").ok()?;
            let replace_all = arguments
                .get("replace_all")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let original = tokio::fs::read_to_string(&path).await.ok()?;
            let (updated, _) = replace_text(&original, old_text, new_text, replace_all).ok()?;
            Some(PersistedDiff {
                path: path.to_str()?.to_string(),
                old_text: Some(original),
                new_text: updated,
            })
        }
        "write_file" => {
            let path =
                resolve_writable_path(session, required_string(arguments, "path").ok()?).ok()?;
            let content = required_string(arguments, "content").ok()?;
            let old_text = match tokio::fs::read_to_string(&path).await {
                Ok(text) => Some(text),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                // Not text, a directory, or unreadable: showing it as a new
                // file would hide that the write replaces something.
                Err(_) => return None,
            };
            let create_directories = arguments
                .get("create_directories")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            if old_text.is_none() && !create_directories {
                let parent = path.parent()?;
                if !tokio::fs::metadata(parent)
                    .await
                    .is_ok_and(|metadata| metadata.is_dir())
                {
                    return None;
                }
            }
            Some(PersistedDiff {
                path: path.to_str()?.to_string(),
                old_text,
                new_text: content.to_string(),
            })
        }
        _ => None,
    }
}

async fn run_command(
    access: &ToolAccess,
    session: &SessionRecord,
    arguments: &Value,
    terminal_call: Option<&TerminalToolCall>,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let command = required_string(arguments, "command")?;
    let args = arguments
        .get("args")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let cwd = match arguments.get("cwd").and_then(Value::as_str) {
        Some(cwd) => resolve_existing_path(session, cwd)?,
        None => session.cwd.clone(),
    };
    let timeout = Duration::from_secs(
        arguments
            .get("timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(120)
            .clamp(1, 1800),
    );

    if access.client.terminal
        && let Some(connection) = access.connection.as_ref()
    {
        return run_command_via_client(
            access,
            connection,
            session,
            command,
            &args,
            cwd,
            timeout,
            terminal_call,
            cancellation,
        )
        .await;
    }

    let mut process = tokio::process::Command::new(command);
    process
        .args(&args)
        .current_dir(&cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process
        .spawn()
        .map_err(agent_client_protocol::Error::into_internal_error)?;
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let output_limit = access.config.tool_output_bytes;

    let wait = async {
        let (status, stdout, stderr) = tokio::join!(
            child.wait(),
            read_limited(stdout.as_mut(), output_limit),
            read_limited(stderr.as_mut(), output_limit),
        );
        Ok::<_, agent_client_protocol::Error>((status, stdout, stderr))
    };

    let result = tokio::select! {
        _ = cancellation.cancelled() => {
            let _ = child.kill().await;
            return Err(agent_client_protocol::Error::request_cancelled());
        }
        result = tokio::time::timeout(timeout, wait) => {
            match result {
                Ok(result) => result?,
                Err(_) => {
                    let _ = child.kill().await;
                    return Err(agent_client_protocol::Error::internal_error()
                        .data(format!("command timed out after {} seconds", timeout.as_secs())));
                }
            }
        }
    };
    let (status, stdout, stderr) = result;
    let status = status.map_err(agent_client_protocol::Error::into_internal_error)?;
    let text = format!(
        "exit_code: {}\nstdout:\n{}\nstderr:\n{}",
        status
            .code()
            .map_or_else(|| "signal".to_string(), |code| code.to_string()),
        stdout,
        stderr
    );
    Ok(ToolResult {
        text: truncate(&text, output_limit),
        raw: json!({
            "command": command,
            "args": args,
            "cwd": cwd,
            "exit_code": status.code(),
            "stdout": stdout,
            "stderr": stderr
        }),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_command_via_client(
    access: &ToolAccess,
    connection: &ConnectionTo<Client>,
    session: &SessionRecord,
    command: &str,
    args: &[String],
    cwd: PathBuf,
    timeout: Duration,
    terminal_call: Option<&TerminalToolCall>,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    let env = Vec::<EnvVariable>::new();
    let request = CreateTerminalRequest::new(session.id.clone(), command)
        .args(args.to_vec())
        .env(env)
        .cwd(cwd.clone())
        .output_byte_limit(Some(access.config.tool_output_bytes as u64));
    let created = tokio::select! {
        _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
        response = connection.send_request(request).block_task() => response?,
    };
    let terminal_id = created.terminal_id;
    if let Some(tool_call) = terminal_call {
        let _ = tool_call.terminal_id.set(terminal_id.clone());
        // ACP clients show a terminal's live output in the tool call that
        // embeds it. A terminal that no tool call embeds may get a tool call
        // of its own, which shows the command twice.
        let update = ToolCallUpdate::new(
            tool_call.id.clone(),
            ToolCallUpdateFields::new()
                .status(ToolCallStatus::InProgress)
                .content(vec![ToolCallContent::Terminal(Terminal::new(
                    terminal_id.clone(),
                ))]),
        );
        if let Err(error) = connection.send_notification(SessionNotification::new(
            session.id.clone(),
            SessionUpdate::ToolCallUpdate(update),
        )) {
            tracing::warn!(
                ?error,
                "failed to show a command's terminal in its tool call"
            );
        }
    }

    let wait = tokio::time::timeout(timeout, async {
        tokio::select! {
            _ = cancellation.cancelled() => Err(agent_client_protocol::Error::request_cancelled()),
            response = connection
                .send_request(WaitForTerminalExitRequest::new(session.id.clone(), terminal_id.clone()))
                .block_task() => response,
        }
    })
    .await;
    let exit = match wait {
        Ok(Ok(exit)) => exit,
        Ok(Err(error)) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
            let _ = connection
                .send_request(KillTerminalRequest::new(
                    session.id.clone(),
                    terminal_id.clone(),
                ))
                .block_task()
                .await;
            let _ = connection
                .send_request(ReleaseTerminalRequest::new(session.id.clone(), terminal_id))
                .block_task()
                .await;
            return Err(error);
        }
        Ok(Err(error)) => {
            let _ = connection
                .send_request(KillTerminalRequest::new(
                    session.id.clone(),
                    terminal_id.clone(),
                ))
                .block_task()
                .await;
            let _ = connection
                .send_request(ReleaseTerminalRequest::new(session.id.clone(), terminal_id))
                .block_task()
                .await;
            return Err(error);
        }
        Err(_) => {
            let _ = connection
                .send_request(KillTerminalRequest::new(
                    session.id.clone(),
                    terminal_id.clone(),
                ))
                .block_task()
                .await;
            let _ = connection
                .send_request(ReleaseTerminalRequest::new(session.id.clone(), terminal_id))
                .block_task()
                .await;
            return Err(agent_client_protocol::Error::internal_error().data(format!(
                "command timed out after {} seconds",
                timeout.as_secs()
            )));
        }
    };
    let output_result = connection
        .send_request(TerminalOutputRequest::new(
            session.id.clone(),
            terminal_id.clone(),
        ))
        .block_task()
        .await;
    let _ = connection
        .send_request(ReleaseTerminalRequest::new(session.id.clone(), terminal_id))
        .block_task()
        .await;
    let output = output_result?;
    let text = format!(
        "exit_code: {}\noutput:\n{}",
        exit.exit_status
            .exit_code
            .map_or_else(|| "signal".to_string(), |code| code.to_string()),
        output.output
    );
    Ok(ToolResult {
        text: truncate(&text, access.config.tool_output_bytes),
        raw: json!({
            "command": command,
            "args": args,
            "cwd": cwd,
            "exit_code": exit.exit_status.exit_code,
            "signal": exit.exit_status.signal,
            "output": output.output,
            "truncated": output.truncated
        }),
    })
}

async fn web_fetch(
    access: &ToolAccess,
    session: &SessionRecord,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, agent_client_protocol::Error> {
    if access.config.disable_web || !session.config.network {
        return Err(agent_client_protocol::Error::invalid_request()
            .data("network access is disabled for this session"));
    }
    let url = required_string(arguments, "url")?;
    let parsed = reqwest::Url::parse(url)
        .map_err(|error| agent_client_protocol::Error::invalid_params().data(error.to_string()))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(agent_client_protocol::Error::invalid_params()
            .data("only http and https URLs are supported"));
    }
    validate_public_fetch_url(&parsed, cancellation).await?;
    let max_bytes = arguments
        .get("max_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(262_144)
        .clamp(1024, 1_048_576) as usize;
    let response = tokio::select! {
        _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
        response = reqwest::Client::builder()
            .timeout(access.config.http_timeout)
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 || is_private_host(attempt.url()) {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }))
            .user_agent(concat!("dsh-acp/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(agent_client_protocol::Error::into_internal_error)?
            .get(parsed.clone())
            .send() => response.map_err(agent_client_protocol::Error::into_internal_error)?,
    };
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let redirect_location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(max_bytes.min(32 * 1024));
    while bytes.len() < max_bytes {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(agent_client_protocol::Error::into_internal_error)?;
        let take = (max_bytes - bytes.len()).min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            break;
        }
    }
    let body = String::from_utf8_lossy(&bytes).to_string();
    let mut text = if content_type.contains("html") {
        html_to_text(&body)
    } else {
        body
    };
    if status.is_redirection()
        && let Some(location) = &redirect_location
    {
        text = format!("HTTP redirect to {location}\n{text}");
    }
    let text = truncate(&text, access.config.tool_output_bytes);
    Ok(ToolResult {
        text: text.clone(),
        raw: json!({
            "url": parsed.as_str(),
            "status": status.as_u16(),
            "content_type": content_type,
            "location": redirect_location,
            "content": text
        }),
    })
}

async fn validate_public_fetch_url(
    url: &reqwest::Url,
    cancellation: &CancellationToken,
) -> Result<(), agent_client_protocol::Error> {
    if is_private_host(url) {
        return Err(agent_client_protocol::Error::invalid_params()
            .data("web_fetch does not allow localhost or private network addresses"));
    }
    let Some(host) = url.host_str() else {
        return Err(agent_client_protocol::Error::invalid_params().data("URL has no host"));
    };
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(());
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let addresses = tokio::select! {
        _ = cancellation.cancelled() => return Err(agent_client_protocol::Error::request_cancelled()),
        result = tokio::net::lookup_host((host, port)) => result.map_err(agent_client_protocol::Error::into_internal_error)?,
    };
    if addresses
        .into_iter()
        .any(|address| is_private_ip(address.ip()))
    {
        return Err(agent_client_protocol::Error::invalid_params()
            .data("web_fetch host resolves to a private network address"));
    }
    Ok(())
}

fn is_private_host(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return true;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        return true;
    }
    host.parse::<std::net::IpAddr>().is_ok_and(is_private_ip)
}

fn is_private_ip(address: std::net::IpAddr) -> bool {
    match address {
        std::net::IpAddr::V4(address) => {
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_multicast()
        }
        std::net::IpAddr::V6(address) => {
            address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || (address.segments()[0] & 0xfe00) == 0xfc00
                || (address.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

fn resolve_existing_path(
    session: &SessionRecord,
    path: &str,
) -> Result<PathBuf, agent_client_protocol::Error> {
    let path = candidate_path(session, path);
    let canonical = path.canonicalize().map_err(|error| {
        agent_client_protocol::Error::resource_not_found(Some(path.display().to_string()))
            .data(error.to_string())
    })?;
    ensure_in_roots(session, &canonical)?;
    Ok(canonical)
}

fn resolve_writable_path(
    session: &SessionRecord,
    path: &str,
) -> Result<PathBuf, agent_client_protocol::Error> {
    let path = normalize_lexically(&candidate_path(session, path));
    let existing_ancestor = path
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .ok_or_else(|| {
            agent_client_protocol::Error::invalid_params().data("path has no existing ancestor")
        })?;
    let canonical_ancestor = existing_ancestor
        .canonicalize()
        .map_err(agent_client_protocol::Error::into_internal_error)?;
    ensure_in_roots(session, &canonical_ancestor)?;
    Ok(path)
}

fn candidate_path(session: &SessionRecord, path: &str) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        session.cwd.join(path)
    }
}

fn ensure_in_roots(
    session: &SessionRecord,
    path: &Path,
) -> Result<(), agent_client_protocol::Error> {
    let mut roots = Vec::with_capacity(1 + session.additional_directories.len());
    roots.push(session.cwd.as_path());
    roots.extend(session.additional_directories.iter().map(PathBuf::as_path));
    if roots.iter().any(|root| {
        root.canonicalize()
            .is_ok_and(|root| path == root || path.starts_with(root))
    }) {
        Ok(())
    } else {
        Err(agent_client_protocol::Error::invalid_params().data(format!(
            "path {} is outside the session workspace roots",
            path.display()
        )))
    }
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn is_noisy_directory(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some(".git" | "target" | "node_modules" | ".dsh-acp")
        )
    })
}

fn is_hidden_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str().to_string_lossy().starts_with('.'))
}

fn required_string<'a>(
    arguments: &'a Value,
    key: &str,
) -> Result<&'a str, agent_client_protocol::Error> {
    arguments.get(key).and_then(Value::as_str).ok_or_else(|| {
        agent_client_protocol::Error::invalid_params().data(format!("missing string field {key}"))
    })
}

fn truncate(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!(
        "{}\n\n[... output truncated at {} bytes ...]",
        &value[..boundary],
        max_bytes
    )
}

fn session_output_budget(_session: &SessionRecord, fallback: usize) -> usize {
    fallback
}

async fn read_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: Option<&mut R>,
    limit: usize,
) -> String {
    let Some(reader) = reader else {
        return String::new();
    };
    let mut buffer = Vec::with_capacity(limit.min(8192));
    let _ = reader.take(limit as u64).read_to_end(&mut buffer).await;
    String::from_utf8_lossy(&buffer).to_string()
}

fn html_to_text(html: &str) -> String {
    let script = regex::Regex::new(r"(?is)<script\b[^>]*>.*?</script>").unwrap();
    let style = regex::Regex::new(r"(?is)<style\b[^>]*>.*?</style>").unwrap();
    let noscript = regex::Regex::new(r"(?is)<noscript\b[^>]*>.*?</noscript>").unwrap();
    let tags = regex::Regex::new(r"(?s)<[^>]+>").unwrap();
    let whitespace = regex::Regex::new(r"[ \t\r\f\v]+").unwrap();
    let blank_lines = regex::Regex::new(r"\n{3,}").unwrap();
    let text = script.replace_all(html, " ");
    let text = style.replace_all(&text, " ");
    let text = noscript.replace_all(&text, " ");
    let text = tags.replace_all(&text, " ");
    let text = text
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    blank_lines
        .replace_all(&whitespace.replace_all(&text, " "), "\n\n")
        .trim()
        .to_string()
}

/// What an "allow for this session" answer remembers, scoped as Codex scopes
/// it: a command by its exact program, arguments and working directory; a
/// write or edit by the file; an MCP call by the tool. Approving `ls` must not
/// approve every later command.
pub fn session_approval_key(session: &SessionRecord, tool: &str, arguments: &Value) -> String {
    match tool {
        "run_command" => json!({
            "tool": tool,
            "command": arguments.get("command"),
            "args": arguments.get("args"),
            "cwd": arguments.get("cwd"),
        }),
        "write_file" | "edit_file" => json!({
            "tool": "file",
            "path": arguments
                .get("path")
                .and_then(Value::as_str)
                .map(|path| lexical_path(&session.cwd, path)),
        }),
        _ => json!({"tool": tool}),
    }
    .to_string()
}

/// The label of the "allow for this session" option, naming what it covers.
pub fn session_approval_label(tool: &str) -> &'static str {
    match tool {
        "run_command" => "Yes, and don't ask again for this command in this session",
        "write_file" | "edit_file" => "Yes, and don't ask again for this file in this session",
        _ => "Allow for this session",
    }
}

pub fn is_auto_approved(
    session: &SessionRecord,
    approved: &std::collections::HashSet<String>,
    key: &str,
) -> bool {
    session.config.permission_mode == PermissionMode::Allow || approved.contains(key)
}

/// `path` relative to `cwd` with `.` and `..` removed, without touching the
/// filesystem, so a file approved for the session keeps one key whether it
/// exists yet or not.
fn lexical_path(cwd: &Path, path: &str) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in cwd.join(path).components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other),
        }
    }
    normal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReasoningEffort;

    #[test]
    fn error_reason_prefers_data_over_the_error_name() {
        use agent_client_protocol::Error;
        assert_eq!(
            error_reason(&Error::invalid_params().data("missing path")),
            "missing path"
        );
        assert_eq!(
            error_reason(&Error::internal_error().data(json!({"status": 502}))),
            r#"{"status":502}"#
        );
        assert_eq!(error_reason(&Error::invalid_params()), "Invalid params");
        assert_eq!(
            error_reason(&Error::invalid_params().data(Value::Null)),
            "Invalid params"
        );
        assert_eq!(
            error_reason(&Error::invalid_params().data(" ")),
            "Invalid params"
        );
        let mut explained = Error::internal_error().data(json!({"resultType": "input_required"}));
        explained.message = "MCP server asked for more input".to_string();
        assert_eq!(error_reason(&explained), "MCP server asked for more input");
    }

    #[test]
    fn html_conversion_removes_scripts_and_tags() {
        let output = html_to_text(
            "<html><style>x{}</style><body><h1>Hello</h1><script>bad()</script><p>world &amp; co</p></body></html>",
        );
        assert!(output.contains("Hello"));
        assert!(output.contains("world & co"));
        assert!(!output.contains("bad()"));
        assert!(!output.contains("x{}"));
    }

    #[test]
    fn lexical_normalization_stays_absolute() {
        assert_eq!(
            normalize_lexically(Path::new("/a/b/../c")),
            PathBuf::from("/a/c")
        );
    }

    #[test]
    fn web_fetch_rejects_private_addresses() {
        assert!(is_private_host(
            &reqwest::Url::parse("http://127.0.0.1:8080").unwrap()
        ));
        assert!(is_private_host(
            &reqwest::Url::parse("http://localhost/private").unwrap()
        ));
        assert!(!is_private_host(
            &reqwest::Url::parse("https://example.com/docs").unwrap()
        ));
    }

    #[test]
    fn git_patch_uses_absolute_paths_and_git_format() {
        let diff = PersistedDiff {
            path: "/workspace/src/lib.rs".to_string(),
            old_text: Some("old\n".to_string()),
            new_text: "new\n".to_string(),
        };
        let patch = git_patch(&diff);
        assert!(patch.starts_with("diff --git /workspace/src/lib.rs"));
        assert!(patch.contains("--- /workspace/src/lib.rs"));
        assert!(patch.contains("+++ /workspace/src/lib.rs"));
        assert!(patch.contains("-old\n+new\n"));
    }

    fn session_in(cwd: &Path) -> SessionRecord {
        SessionRecord::new(
            cwd.to_path_buf(),
            Vec::new(),
            "deepseek-flash".to_string(),
            PermissionMode::Ask,
            ReasoningEffort::High,
            None,
            None,
        )
    }

    #[tokio::test]
    async fn edit_preview_applies_the_edit_rules_without_writing() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().canonicalize().unwrap();
        std::fs::write(cwd.join("a.txt"), "one two two\n").unwrap();
        let session = session_in(&cwd);
        let edit = |old: &str, replace_all: bool| json!({"path": "a.txt", "old_text": old, "new_text": "2", "replace_all": replace_all});

        let diff = preview_diff(&session, "edit_file", &edit("one", false))
            .await
            .unwrap();
        assert_eq!(diff.path, cwd.join("a.txt").to_str().unwrap());
        assert_eq!(diff.old_text.as_deref(), Some("one two two\n"));
        assert_eq!(diff.new_text, "2 two two\n");
        let all = preview_diff(&session, "edit_file", &edit("two", true))
            .await
            .unwrap();
        assert_eq!(all.new_text, "one 2 2\n");
        assert_eq!(
            std::fs::read_to_string(cwd.join("a.txt")).unwrap(),
            "one two two\n"
        );

        // Ambiguous, missing text, missing file: the tool would fail.
        assert!(
            preview_diff(&session, "edit_file", &edit("two", false))
                .await
                .is_none()
        );
        assert!(
            preview_diff(&session, "edit_file", &edit("three", false))
                .await
                .is_none()
        );
        let missing = json!({"path": "b.txt", "old_text": "x", "new_text": "y"});
        assert!(
            preview_diff(&session, "edit_file", &missing)
                .await
                .is_none()
        );
        std::fs::write(cwd.join("binary.bin"), [0xff, 0xfe, b'x']).unwrap();
        let binary = json!({"path": "binary.bin", "old_text": "x", "new_text": "y"});
        assert!(preview_diff(&session, "edit_file", &binary).await.is_none());
    }

    #[tokio::test]
    async fn write_preview_shows_the_current_file_or_none() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().canonicalize().unwrap();
        std::fs::write(cwd.join("a.txt"), "old\n").unwrap();
        let session = session_in(&cwd);
        let write = |path: &str| json!({"path": path, "content": "new\n"});

        let replaced = preview_diff(&session, "write_file", &write("a.txt"))
            .await
            .unwrap();
        assert_eq!(replaced.old_text.as_deref(), Some("old\n"));
        assert_eq!(replaced.new_text, "new\n");
        let created = preview_diff(&session, "write_file", &write("sub/b.txt"))
            .await
            .unwrap();
        assert_eq!(created.path, cwd.join("sub/b.txt").to_str().unwrap());
        assert_eq!(created.old_text, None);
        assert!(!cwd.join("sub").exists());
        let no_parent = json!({"path": "sub/b.txt", "content": "x", "create_directories": false});
        assert!(
            preview_diff(&session, "write_file", &no_parent)
                .await
                .is_none()
        );
        std::fs::write(cwd.join("binary.bin"), [0xff, 0xfe]).unwrap();
        assert!(
            preview_diff(&session, "write_file", &write("binary.bin"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn preview_reads_nothing_outside_the_workspace_roots() {
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().canonicalize().unwrap().join("secret.txt");
        std::fs::write(&secret, "secret\n").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().canonicalize().unwrap();
        let session = session_in(&cwd);
        let secret_path = secret.to_str().unwrap();

        let edit = json!({"path": secret_path, "old_text": "secret", "new_text": "x"});
        assert!(preview_diff(&session, "edit_file", &edit).await.is_none());
        let write = json!({"path": secret_path, "content": "x"});
        assert!(preview_diff(&session, "write_file", &write).await.is_none());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&secret, cwd.join("link.txt")).unwrap();
            let edit = json!({"path": "link.txt", "old_text": "secret", "new_text": "x"});
            assert!(preview_diff(&session, "edit_file", &edit).await.is_none());
            let write = json!({"path": "link.txt", "content": "x"});
            assert!(preview_diff(&session, "write_file", &write).await.is_none());
        }
    }

    #[test]
    fn failed_spawn_does_not_report_an_exited_terminal() {
        let terminal = persisted_terminal(
            "run_command",
            &json!({
                "_terminal_id": "term-1",
                "command": "missing",
                "cwd": "/workspace",
                "error": "not found"
            }),
        )
        .unwrap();
        assert!(!terminal.exited);
        assert_eq!(terminal.command, "missing");
    }
}
