use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type", default = "tool_type")]
    pub kind: String,
    pub function: FunctionCall,
}

fn tool_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
    },
    ImageUrl {
        image_url: ImageUrl,
    },
    File {
        #[serde(skip_serializing_if = "Option::is_none")]
        file_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_data: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageUrl {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl MessageContent {
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    pub fn text_value(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ContentPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
        }
    }

    pub fn approximate_chars(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Parts(parts) => parts
                .iter()
                .map(|part| match part {
                    ContentPart::Text { text } => text.len(),
                    ContentPart::ImageUrl { image_url } => image_url.url.len(),
                    ContentPart::File {
                        file_id,
                        file_data,
                        filename,
                    } => {
                        file_id.as_deref().map_or(0, str::len)
                            + file_data.as_deref().map_or(0, str::len)
                            + filename.as_deref().map_or(0, str::len)
                    }
                })
                .sum(),
        }
    }
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: Some(MessageContent::text(content)),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: Some(MessageContent::text(content)),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn user_parts(parts: Vec<ContentPart>) -> Self {
        let content = if parts
            .iter()
            .all(|part| matches!(part, ContentPart::Text { .. }))
            && parts.len() == 1
        {
            match parts.into_iter().next() {
                Some(ContentPart::Text { text }) => MessageContent::Text(text),
                _ => unreachable!("single text part"),
            }
        } else {
            MessageContent::Parts(parts)
        };
        Self {
            role: Role::User,
            content: Some(content),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn user_content(content: MessageContent) -> Self {
        Self {
            role: Role::User,
            content: Some(content),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant(content: Option<String>, reasoning_content: Option<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.map(MessageContent::text),
            reasoning_content,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: Some(MessageContent::text(content)),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
        }
    }

    pub fn text_content(&self) -> Option<&str> {
        match self.content.as_ref() {
            Some(MessageContent::Text(text)) => Some(text),
            _ => None,
        }
    }

    pub fn approximate_chars(&self) -> usize {
        self.content
            .as_ref()
            .map_or(0, MessageContent::approximate_chars)
            + self.reasoning_content.as_deref().map_or(0, str::len)
            + self
                .tool_calls
                .iter()
                .map(|call| {
                    call.function.name.len() + call.function.arguments.len() + call.id.len() + 24
                })
                .sum::<usize>()
            + self.tool_call_id.as_deref().map_or(0, str::len)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub prompt_cache_hit_tokens: u64,
    pub prompt_cache_miss_tokens: u64,
}

impl TokenUsage {
    pub fn saturating_add(&mut self, other: &Self) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(other.prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(other.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.prompt_cache_hit_tokens = self
            .prompt_cache_hit_tokens
            .saturating_add(other.prompt_cache_hit_tokens);
        self.prompt_cache_miss_tokens = self
            .prompt_cache_miss_tokens
            .saturating_add(other.prompt_cache_miss_tokens);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionConfig {
    pub model: String,
    pub mode: String,
    pub permission_mode: crate::config::PermissionMode,
    pub network: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedDiff {
    pub path: String,
    pub old_text: Option<String>,
    pub new_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedTerminal {
    pub terminal_id: String,
    pub command: String,
    pub cwd: std::path::PathBuf,
    pub output: String,
    pub exit_code: Option<u32>,
    pub signal: Option<String>,
    pub exited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedToolOutcome {
    pub status: ToolOutcomeStatus,
    pub text: String,
    pub raw_output: serde_json::Value,
    #[serde(default)]
    pub structured_content: Vec<serde_json::Value>,
    #[serde(default)]
    pub diff: Option<PersistedDiff>,
    #[serde(default)]
    pub terminal: Option<PersistedTerminal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedCompaction {
    pub id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PendingUserMessage {
    pub message_id: String,
    pub content: MessageContent,
    #[serde(default)]
    pub blocks: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub cwd: std::path::PathBuf,
    #[serde(default)]
    pub additional_directories: Vec<std::path::PathBuf>,
    #[serde(default)]
    pub title: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub config: SessionConfig,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub prompt_blocks: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub message_ids: BTreeMap<String, String>,
    #[serde(default)]
    pub pending_steering: Vec<PendingUserMessage>,
    #[serde(default)]
    pub tool_outcomes: BTreeMap<String, PersistedToolOutcome>,
    #[serde(default)]
    pub compactions: Vec<PersistedCompaction>,
    #[serde(default)]
    pub usage: TokenUsage,
    #[serde(default)]
    pub context_used: u64,
    #[serde(default)]
    pub parent_session_id: Option<String>,
}

impl SessionRecord {
    pub fn new(
        cwd: std::path::PathBuf,
        additional_directories: Vec<std::path::PathBuf>,
        default_model: String,
        permission_mode: crate::config::PermissionMode,
        system_prompt: Option<String>,
        parent_session_id: Option<String>,
    ) -> Self {
        let now = chrono::Utc::now();
        Self {
            id: format!("dsh-{}", Uuid::new_v4().simple()),
            cwd,
            additional_directories,
            title: None,
            created_at: now,
            updated_at: now,
            config: SessionConfig {
                model: default_model,
                mode: "agent".to_string(),
                permission_mode,
                network: true,
            },
            system_prompt,
            messages: Vec::new(),
            prompt_blocks: BTreeMap::new(),
            message_ids: BTreeMap::new(),
            pending_steering: Vec::new(),
            tool_outcomes: BTreeMap::new(),
            compactions: Vec::new(),
            usage: TokenUsage::default(),
            context_used: 0,
            parent_session_id,
        }
    }

    pub fn install_system_prompt(&mut self, prompt: String) {
        self.messages.retain(|message| message.role != Role::System);
        self.messages.insert(0, ChatMessage::system(prompt));
    }

    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now();
    }

    pub fn flush_pending_steering(&mut self) {
        let pending = std::mem::take(&mut self.pending_steering);
        for message in pending {
            let index = self.messages.len();
            self.messages
                .push(ChatMessage::user_content(message.content));
            self.prompt_blocks.insert(index.to_string(), message.blocks);
            self.message_ids
                .insert(index.to_string(), message.message_id);
        }
    }

    pub fn context_messages(&self, budget_chars: usize) -> Vec<ChatMessage> {
        let mut system = Vec::new();
        let mut rest = Vec::new();
        for message in &self.messages {
            if message.role == Role::System {
                system.push(message.clone());
            } else {
                rest.push(message.clone());
            }
        }

        let system_chars = system
            .iter()
            .map(ChatMessage::approximate_chars)
            .sum::<usize>();
        let mut remaining = budget_chars.saturating_sub(system_chars);
        let mut start = rest.len();
        while start > 0 {
            let size = rest[start - 1].approximate_chars();
            if size > remaining && !rest.is_empty() {
                break;
            }
            remaining = remaining.saturating_sub(size);
            start -= 1;
        }

        // A tool result must never be separated from the assistant message that
        // requested it. Move the boundary backwards until it is a user message.
        while start < rest.len() && rest[start].role == Role::Tool {
            start = start.saturating_sub(1);
            if start == 0 {
                break;
            }
        }
        if start < rest.len() && rest[start].role == Role::Tool {
            start = rest.len();
        }
        if start == rest.len() {
            start = rest
                .iter()
                .rposition(|message| message.role == Role::User)
                .unwrap_or(rest.len());
        }

        system.extend(rest.into_iter().skip(start).map(|mut message| {
            // reasoning_content is a response-only DeepSeek field. Keep it in
            // the persisted transcript for replay and thought rendering, but
            // do not send it back in the next completion request.
            message.reasoning_content = None;
            message
        }));
        system
    }
}

#[derive(Debug, Clone)]
pub struct SessionRuntime {
    pub state: Arc<tokio::sync::Mutex<SessionState>>,
    pub lifecycle: Arc<tokio::sync::Mutex<()>>,
    pub turn_lock: Arc<tokio::sync::Mutex<()>>,
    pub closed: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug)]
pub struct SessionState {
    pub record: SessionRecord,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub active_prompt: Arc<std::sync::atomic::AtomicBool>,
    pub requires_action: Arc<std::sync::atomic::AtomicBool>,
    pub activity_notify: Arc<tokio::sync::Notify>,
    pub prompt_seq: u64,
    pub approved_tools: std::collections::HashSet<String>,
    pub mcp: Option<crate::mcp::McpSession>,
}

impl SessionRuntime {
    pub fn new(record: SessionRecord) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(SessionState {
                record,
                cancellation: tokio_util::sync::CancellationToken::new(),
                active_prompt: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                requires_action: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                activity_notify: Arc::new(tokio::sync::Notify::new()),
                prompt_seq: 0,
                approved_tools: std::collections::HashSet::new(),
                mcp: None,
            })),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            turn_lock: Arc::new(tokio::sync::Mutex::new(())),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub async fn snapshot(&self) -> SessionRecord {
        self.state.lock().await.record.clone()
    }
}

pub fn ensure_session_unique_tool_call_ids(record: &SessionRecord, calls: &mut [ToolCall]) {
    let mut seen = std::collections::HashSet::new();
    for message in &record.messages {
        for call in &message.tool_calls {
            seen.insert(call.id.clone());
        }
        if let Some(id) = &message.tool_call_id {
            seen.insert(id.clone());
        }
    }
    for call in calls {
        if call.id.is_empty() || !seen.insert(call.id.clone()) {
            call.id = format!("call_{}", Uuid::new_v4().simple());
            seen.insert(call.id.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PermissionMode;

    fn record() -> SessionRecord {
        SessionRecord::new(
            std::env::current_dir().unwrap(),
            Vec::new(),
            "deepseek-flash".to_string(),
            PermissionMode::Ask,
            None,
            None,
        )
    }

    #[test]
    fn api_context_omits_response_only_reasoning_content() {
        let mut record = record();
        let mut assistant =
            ChatMessage::assistant(Some("answer".to_string()), Some("thought".to_string()));
        assistant.tool_calls.push(ToolCall {
            id: "call-1".to_string(),
            kind: "function".to_string(),
            function: FunctionCall {
                name: "read_file".to_string(),
                arguments: "{}".to_string(),
            },
        });
        record.messages.push(assistant);
        let context = record.context_messages(10_000);
        assert_eq!(context[0].reasoning_content, None);
        assert_eq!(context[0].tool_calls.len(), 1);
    }

    #[test]
    fn oversized_context_still_contains_latest_user_turn() {
        let mut record = record();
        record.messages.push(ChatMessage::user("old"));
        record
            .messages
            .push(ChatMessage::assistant(Some("old answer".to_string()), None));
        record.messages.push(ChatMessage::user("latest request"));
        let context = record.context_messages(1);
        assert_eq!(
            context.last().and_then(ChatMessage::text_content),
            Some("latest request")
        );
    }

    #[test]
    fn tool_call_ids_are_remapped_when_reused_across_turns() {
        let mut record = record();
        let mut previous = ChatMessage::assistant(None, None);
        previous.tool_calls.push(ToolCall {
            id: "call-existing".to_string(),
            kind: "function".to_string(),
            function: FunctionCall {
                name: "read_file".to_string(),
                arguments: "{}".to_string(),
            },
        });
        record.messages.push(previous);
        let mut calls = vec![ToolCall {
            id: "call-existing".to_string(),
            kind: "function".to_string(),
            function: FunctionCall {
                name: "read_file".to_string(),
                arguments: "{}".to_string(),
            },
        }];
        ensure_session_unique_tool_call_ids(&record, &mut calls);
        assert_ne!(calls[0].id, "call-existing");
    }
}
