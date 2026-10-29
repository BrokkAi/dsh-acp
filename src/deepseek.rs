use std::collections::BTreeMap;

use agent_client_protocol::Error;
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{Config, ReasoningEffort},
    model::{ChatMessage, FunctionCall, TokenUsage, ToolCall},
};

#[derive(Debug, Clone, Serialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub stream: bool,
    /// DeepSeek thinking effort. `none` turns thinking mode off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<&'static str>,
    /// Omitted by default so the API uses its own maximum. DeepSeek's examples
    /// do not send it either.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<String>,
    pub stream_options: StreamOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamOptions {
    pub include_usage: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Completion {
    pub content: String,
    pub reasoning_content: String,
    pub tool_calls: Vec<ToolCall>,
    pub finish_reason: Option<String>,
    pub usage: TokenUsage,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Text(String),
    Reasoning(String),
    Usage(TokenUsage),
}

#[derive(Clone)]
pub struct DeepSeekClient {
    http: reqwest::Client,
    endpoint: String,
    api_key: Option<String>,
    headers: reqwest::header::HeaderMap,
    max_tokens: Option<u32>,
}

impl DeepSeekClient {
    pub fn new(config: &Config) -> Result<Self, Error> {
        Self::with_routing(
            config,
            config.base_url.clone(),
            std::collections::HashMap::new(),
        )
    }

    pub fn with_routing(
        config: &Config,
        base_url: String,
        headers: std::collections::HashMap<String, String>,
    ) -> Result<Self, Error> {
        if base_url.trim().is_empty() {
            return Err(Error::invalid_params().data("provider base URL must not be empty"));
        }
        let endpoint = {
            let base = base_url.trim_end_matches('/');
            if base.ends_with("/chat/completions") {
                base.to_string()
            } else {
                format!("{base}/chat/completions")
            }
        };
        let mut header_map = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| Error::invalid_params().data(error.to_string()))?;
            let value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|error| Error::invalid_params().data(error.to_string()))?;
            header_map.insert(name, value);
        }
        // A blank key counts as no key, so it is never sent as an empty
        // bearer token.
        let api_key = config.api_key().ok().map(str::to_owned);
        if api_key.is_none() && !header_map.contains_key(reqwest::header::AUTHORIZATION) {
            return Err(Error::auth_required().data(
                "Set DEEPSEEK_API_KEY, pass --api-key, or configure an Authorization provider header",
            ));
        }
        let http = reqwest::Client::builder()
            // A total timeout would also bound the response body, so a
            // completion that is still streaming tokens at the deadline was
            // killed mid-answer. These two bound connection setup and stalls
            // instead: a stream that keeps producing is never cut off.
            .connect_timeout(config.http_timeout)
            .read_timeout(config.http_timeout)
            .user_agent(concat!("dsh-acp/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(Error::into_internal_error)?;
        Ok(Self {
            http,
            endpoint,
            api_key,
            headers: header_map,
            max_tokens: config.max_tokens,
        })
    }

    pub fn request(
        &self,
        model: impl Into<String>,
        effort: ReasoningEffort,
        messages: Vec<ChatMessage>,
        tools: Vec<Value>,
    ) -> ChatRequest {
        let model = model.into();
        ChatRequest {
            model,
            messages,
            stream: true,
            reasoning_effort: Some(effort.as_str()),
            max_tokens: self.max_tokens,
            tools,
            tool_choice: None,
            stream_options: StreamOptions {
                include_usage: true,
            },
        }
    }

    /// Ask for one completion, retrying a failure that a later attempt can fix.
    ///
    /// A retry is only attempted when nothing has been streamed to the client
    /// yet: repeating text the client already rendered would duplicate it, so
    /// a response that dies after its first token is reported instead.
    pub async fn stream_chat(
        &self,
        request: ChatRequest,
        cancellation: &CancellationToken,
        on_event: &mut (dyn FnMut(StreamEvent) + Send),
    ) -> Result<Completion, Error> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let emitted = std::sync::atomic::AtomicBool::new(false);
            let outcome = {
                let mut emit = |event: StreamEvent| {
                    if matches!(&event, StreamEvent::Text(_) | StreamEvent::Reasoning(_)) {
                        emitted.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    on_event(event);
                };
                self.stream_chat_once(&request, cancellation, &mut emit)
                    .await
            };
            let failure = match outcome {
                Ok(completion) => return Ok(completion),
                Err(failure) => failure,
            };
            if !failure.retryable
                || emitted.load(std::sync::atomic::Ordering::Relaxed)
                || attempt >= MAX_ATTEMPTS
            {
                return Err(explain_failure(
                    failure,
                    emitted.load(std::sync::atomic::Ordering::Relaxed),
                ));
            }
            let delay = failure.retry_after.unwrap_or_else(|| backoff(attempt));
            tracing::warn!(
                attempt,
                ?delay,
                error = %failure.error,
                "retrying the model request"
            );
            tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn stream_chat_once(
        &self,
        request: &ChatRequest,
        cancellation: &CancellationToken,
        on_event: &mut (dyn FnMut(StreamEvent) + Send),
    ) -> Result<Completion, AttemptFailure> {
        let mut builder = self
            .http
            .post(&self.endpoint)
            .headers(self.headers.clone())
            .header(
                reqwest::header::ACCEPT,
                "text/event-stream, application/json",
            );
        if let Some(api_key) = &self.api_key
            && !self.headers.contains_key(reqwest::header::AUTHORIZATION)
        {
            builder = builder.bearer_auth(api_key);
        }
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(AttemptFailure::permanent(Error::request_cancelled())),
            response = builder
                .json(request)
                .send() => response.map_err(|error| AttemptFailure::transient(http_error(error)))?,
        };

        let status = response.status();
        if !status.is_success() {
            let retry_after = retry_after(&response);
            let body = match read_provider_prefix(response, PROVIDER_MAX_ERROR_BYTES, cancellation)
                .await
            {
                Ok(body) => body,
                Err(_) if cancellation.is_cancelled() => {
                    return Err(AttemptFailure::permanent(Error::request_cancelled()));
                }
                Err(_) => Vec::new(),
            };
            let body = String::from_utf8_lossy(&body);
            let detail = extract_api_error(&body).unwrap_or_else(|| body.into_owned());
            let message = format!("DeepSeek API returned HTTP {status}: {detail}");
            return Err(
                if status == reqwest::StatusCode::UNAUTHORIZED
                    || status == reqwest::StatusCode::FORBIDDEN
                {
                    let message = format!(
                        "{message}. Check DEEPSEEK_API_KEY (or --api-key, or the provider's Authorization header)."
                    );
                    AttemptFailure::permanent(
                        Error::auth_required()
                            .data(json!({"status": status.as_u16(), "message": message})),
                    )
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
                {
                    AttemptFailure {
                        error: Box::new(
                            Error::internal_error()
                                .data(json!({"status": status.as_u16(), "message": message})),
                        ),
                        retryable: true,
                        retry_after,
                    }
                } else {
                    AttemptFailure::permanent(
                        Error::internal_error()
                            .data(json!({"status": status.as_u16(), "message": message})),
                    )
                },
            );
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if content_type.contains("application/json") {
            let bytes = read_provider_body(
                response,
                PROVIDER_MAX_JSON_BYTES,
                cancellation,
                "DeepSeek JSON response",
            )
            .await?;
            let mut completion = Completion::default();
            ingest_json_response(&bytes, &mut completion, on_event)
                .map_err(AttemptFailure::permanent)?;
            return ensure_not_empty(completion);
        }

        let mut stream = response.bytes_stream();
        let mut byte_buffer = Vec::<u8>::new();
        let mut completion = Completion::default();
        let mut pending_data = String::new();
        let mut saw_data = false;
        let mut saw_done = false;
        let mut streamed_bytes = 0usize;

        loop {
            let next = tokio::select! {
                _ = cancellation.cancelled() => return Err(AttemptFailure::permanent(Error::request_cancelled())),
                next = stream.next() => next,
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|error| AttemptFailure::transient(http_error(error)))?;
            streamed_bytes = streamed_bytes.saturating_add(chunk.len());
            if streamed_bytes > PROVIDER_MAX_STREAM_BYTES {
                return Err(AttemptFailure::permanent(Error::internal_error().data(
                    format!(
                        "DeepSeek response exceeded the {PROVIDER_MAX_STREAM_BYTES}-byte stream limit"
                    ),
                )));
            }
            if byte_buffer.len().saturating_add(chunk.len()) > PROVIDER_MAX_EVENT_BYTES {
                return Err(AttemptFailure::permanent(Error::internal_error().data(
                    format!(
                        "DeepSeek SSE input exceeded the {PROVIDER_MAX_EVENT_BYTES}-byte event limit"
                    ),
                )));
            }
            byte_buffer.extend_from_slice(&chunk);

            while let Some(newline) = byte_buffer.iter().position(|byte| *byte == b'\n') {
                let raw = byte_buffer.drain(..=newline).collect::<Vec<_>>();
                let line = String::from_utf8_lossy(&raw);
                let line = line.trim_end_matches(['\r', '\n']);

                if line.is_empty() {
                    if !pending_data.is_empty() {
                        ingest_stream_data(&pending_data, &mut completion, on_event)
                            .map_err(AttemptFailure::permanent)?;
                        pending_data.clear();
                    }
                    continue;
                }
                if line.starts_with(':') {
                    continue;
                }
                if let Some(data) = line.strip_prefix("data:") {
                    let data = data.trim_start();
                    if data == "[DONE]" {
                        saw_done = true;
                        continue;
                    }
                    saw_data = true;
                    if pending_data
                        .len()
                        .saturating_add(data.len() + usize::from(!pending_data.is_empty()))
                        > PROVIDER_MAX_EVENT_BYTES
                    {
                        return Err(AttemptFailure::permanent(Error::internal_error().data(
                            format!(
                                "DeepSeek SSE event exceeded the {PROVIDER_MAX_EVENT_BYTES}-byte limit"
                            ),
                        )));
                    }
                    if pending_data.is_empty() {
                        pending_data.push_str(data);
                    } else {
                        pending_data.push('\n');
                        pending_data.push_str(data);
                    }
                }
            }
        }

        if !pending_data.is_empty() {
            ingest_stream_data(&pending_data, &mut completion, on_event)
                .map_err(AttemptFailure::permanent)?;
        } else if !saw_data && !byte_buffer.is_empty() {
            // Some compatible gateways label a single JSON response as an SSE
            // stream. Accept it rather than losing an otherwise valid response.
            if byte_buffer.len() > PROVIDER_MAX_JSON_BYTES {
                return Err(AttemptFailure::permanent(Error::internal_error().data(
                    format!(
                        "DeepSeek JSON response exceeded the {PROVIDER_MAX_JSON_BYTES}-byte limit"
                    ),
                )));
            }
            ingest_json_response(&byte_buffer, &mut completion, on_event)
                .map_err(AttemptFailure::permanent)?;
            return ensure_not_empty(completion);
        }

        // A response is complete when the API says it finished. A connection
        // that closes first used to read as a finished answer, so a half
        // written file looked like a completed task.
        if saw_done || completion.finish_reason.is_some() {
            return Ok(completion);
        }
        Err(AttemptFailure::transient(Error::internal_error().data(
            json!({
                "message": if saw_data {
                    "The DeepSeek API closed the connection before the response finished."
                } else {
                    "The DeepSeek API returned an empty response."
                }
            }),
        )))
    }
}

/// How many times one completion may be requested before giving up.
const MAX_ATTEMPTS: u32 = 3;
const PROVIDER_MAX_ERROR_BYTES: usize = 64 * 1024;
const PROVIDER_MAX_JSON_BYTES: usize = 16 * 1024 * 1024;
const PROVIDER_MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;
const PROVIDER_MAX_STREAM_BYTES: usize = 64 * 1024 * 1024;
const PROVIDER_MAX_TOOL_CALLS: usize = 128;

async fn read_provider_body(
    response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
    label: &str,
) -> Result<Vec<u8>, AttemptFailure> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(AttemptFailure::permanent(
            Error::internal_error().data(format!("{label} exceeded the {limit}-byte limit")),
        ));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(limit.min(32 * 1024));
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(AttemptFailure::permanent(Error::request_cancelled()));
            }
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Ok(bytes);
        };
        let chunk = chunk.map_err(|error| AttemptFailure::transient(http_error(error)))?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(AttemptFailure::permanent(
                Error::internal_error().data(format!("{label} exceeded the {limit}-byte limit")),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
}

async fn read_provider_prefix(
    response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, AttemptFailure> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(limit);
    loop {
        let next = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(AttemptFailure::permanent(Error::request_cancelled()));
            }
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            return Ok(bytes);
        };
        let chunk = chunk.map_err(|error| AttemptFailure::transient(http_error(error)))?;
        let take = limit.saturating_sub(bytes.len()).min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
        if take < chunk.len() || bytes.len() == limit {
            return Ok(bytes);
        }
    }
}

/// A failed attempt, and whether asking again could succeed.
struct AttemptFailure {
    // Boxed so the common success path does not carry a large error type.
    error: Box<Error>,
    retryable: bool,
    retry_after: Option<std::time::Duration>,
}

impl AttemptFailure {
    fn permanent(error: Error) -> Self {
        Self {
            error: Box::new(error),
            retryable: false,
            retry_after: None,
        }
    }

    fn transient(error: Error) -> Self {
        Self {
            error: Box::new(error),
            retryable: true,
            retry_after: None,
        }
    }
}

/// A completion that carries nothing is a failed request, not an empty answer.
fn ensure_not_empty(completion: Completion) -> Result<Completion, AttemptFailure> {
    if completion.content.is_empty()
        && completion.reasoning_content.is_empty()
        && completion.tool_calls.is_empty()
    {
        return Err(AttemptFailure::transient(Error::internal_error().data(
            json!({
                "message": "The DeepSeek API returned an empty response."
            }),
        )));
    }
    Ok(completion)
}

fn explain_failure(failure: AttemptFailure, emitted: bool) -> Error {
    if emitted && failure.retryable {
        let message = failure
            .error
            .data
            .as_ref()
            .and_then(|data| data.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("the connection failed");
        return Error::internal_error().data(json!({
            "message": format!("{message}. The answer was already partly streamed, so it was not retried.")
        }));
    }
    *failure.error
}
/// `Retry-After` in seconds, when the API asks for a pause before retrying.
fn retry_after(response: &reqwest::Response) -> Option<std::time::Duration> {
    let seconds = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(std::time::Duration::from_secs(seconds.min(30)))
}

/// Exponential backoff: 500ms, then 1s.
fn backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(500u64 << attempt.saturating_sub(1).min(5))
}

/// A transport failure with its causes, which reqwest's own message omits
/// ("error sending request for url (...)" without "Connection refused"), and
/// the setting to check when the endpoint could not be reached.
fn http_error(error: reqwest::Error) -> Error {
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
    if error.is_connect() {
        message.push_str(". Check DEEPSEEK_BASE_URL (or --base-url) and the network connection.");
    } else if error.is_timeout() {
        message.push_str(". The DeepSeek API did not answer in time.");
    }
    Error::internal_error().data(json!({"message": message}))
}

fn extract_api_error(body: &str) -> Option<String> {
    let value: Value = serde_json::from_str(body).ok()?;
    value
        .pointer("/error/message")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[derive(Default)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: String,
    arguments: String,
}

fn ingest_json_response(
    bytes: &[u8],
    completion: &mut Completion,
    on_event: &mut (dyn FnMut(StreamEvent) + Send),
) -> Result<(), Error> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| Error::internal_error().data(format!("invalid DeepSeek JSON: {error}")))?;
    if let Some(error) = value.get("error") {
        return Err(Error::internal_error().data(error.clone()));
    }

    let mut calls = BTreeMap::new();
    if let Some(message) = value.pointer("/choices/0/message") {
        if let Some(text) = message.get("content").and_then(Value::as_str) {
            completion.content.push_str(text);
            if !text.is_empty() {
                on_event(StreamEvent::Text(text.to_string()));
            }
        }
        if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
            completion.reasoning_content.push_str(reasoning);
            if !reasoning.is_empty() {
                on_event(StreamEvent::Reasoning(reasoning.to_string()));
            }
        }
        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            if tool_calls.len() > PROVIDER_MAX_TOOL_CALLS {
                return Err(Error::internal_error().data(format!(
                    "DeepSeek response exceeded the {PROVIDER_MAX_TOOL_CALLS}-tool-call limit"
                )));
            }
            for (index, call) in tool_calls.iter().enumerate() {
                let mut accumulator = ToolCallAccumulator {
                    id: call
                        .get("id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    ..ToolCallAccumulator::default()
                };
                if let Some(function) = call.get("function") {
                    accumulator.name = function
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    accumulator.arguments = function
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                calls.insert(index, accumulator);
            }
        }
    }
    completion.finish_reason = value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    completion.usage = parse_usage(value.get("usage"));
    on_event(StreamEvent::Usage(completion.usage.clone()));
    completion.tool_calls = finish_tool_calls(calls);
    Ok(())
}

fn ingest_stream_data(
    data: &str,
    completion: &mut Completion,
    on_event: &mut (dyn FnMut(StreamEvent) + Send),
) -> Result<(), Error> {
    if data.trim().is_empty() {
        return Ok(());
    }
    let value: Value = serde_json::from_str(data).map_err(|error| {
        Error::internal_error().data(format!("invalid DeepSeek SSE event: {error}; data={data}"))
    })?;
    if let Some(error) = value.get("error") {
        return Err(Error::internal_error().data(error.clone()));
    }

    if let Some(choice) = value.pointer("/choices/0") {
        if let Some(delta) = choice.get("delta") {
            if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
                completion.reasoning_content.push_str(reasoning);
                if !reasoning.is_empty() {
                    on_event(StreamEvent::Reasoning(reasoning.to_string()));
                }
            }
            if let Some(text) = delta.get("content").and_then(Value::as_str) {
                completion.content.push_str(text);
                if !text.is_empty() {
                    on_event(StreamEvent::Text(text.to_string()));
                }
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    let raw_index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let Ok(index) = usize::try_from(raw_index) else {
                        return Err(Error::internal_error()
                            .data(format!("DeepSeek tool call index {raw_index} is too large")));
                    };
                    if index >= PROVIDER_MAX_TOOL_CALLS {
                        return Err(Error::internal_error().data(format!(
                            "DeepSeek tool call index {index} exceeded the {PROVIDER_MAX_TOOL_CALLS}-call limit"
                        )));
                    }
                    // The public completion object is the source of truth. A
                    // parallel accumulator is created on first use below.
                    if completion.tool_calls.len() <= index {
                        completion.tool_calls.resize_with(index + 1, || ToolCall {
                            id: String::new(),
                            kind: "function".to_string(),
                            function: FunctionCall {
                                name: String::new(),
                                arguments: String::new(),
                            },
                        });
                    }
                    let target = &mut completion.tool_calls[index];
                    if let Some(id) = call.get("id").and_then(Value::as_str) {
                        target.id.push_str(id);
                    }
                    if let Some(function) = call.get("function") {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            target.function.name.push_str(name);
                        }
                        if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                            target.function.arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            completion.finish_reason = Some(reason.to_string());
        }
    }

    if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
        completion.usage = parse_usage(Some(usage));
        on_event(StreamEvent::Usage(completion.usage.clone()));
    }
    Ok(())
}

fn finish_tool_calls(calls: BTreeMap<usize, ToolCallAccumulator>) -> Vec<ToolCall> {
    calls
        .into_iter()
        .map(|(index, accumulator)| ToolCall {
            id: accumulator
                .id
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| format!("call_{index}_{}", uuid::Uuid::new_v4().simple())),
            kind: "function".to_string(),
            function: FunctionCall {
                name: accumulator.name,
                arguments: if accumulator.arguments.is_empty() {
                    "{}".to_string()
                } else {
                    accumulator.arguments
                },
            },
        })
        .collect()
}

fn parse_usage(value: Option<&Value>) -> TokenUsage {
    let Some(value) = value else {
        return TokenUsage::default();
    };
    TokenUsage {
        prompt_tokens: u64_field(value, "prompt_tokens"),
        completion_tokens: u64_field(value, "completion_tokens"),
        total_tokens: u64_field(value, "total_tokens"),
        prompt_cache_hit_tokens: u64_field(value, "prompt_cache_hit_tokens")
            .max(u64_field(value, "/prompt_tokens_details/cached_tokens")),
        prompt_cache_miss_tokens: u64_field(value, "prompt_cache_miss_tokens"),
    }
}

fn u64_field(value: &Value, pointer: &str) -> u64 {
    let pointer = if pointer.starts_with('/') {
        pointer.to_string()
    } else {
        format!("/{pointer}")
    };
    value.pointer(&pointer).and_then(Value::as_u64).unwrap_or(0)
}

pub fn sanitize_tool_calls(mut completion: Completion) -> Completion {
    let mut seen = std::collections::HashSet::new();
    completion.tool_calls.retain_mut(|call| {
        if call.function.name.is_empty() {
            return false;
        }
        if call.id.is_empty() || !seen.insert(call.id.clone()) {
            call.id = format!("call_{}", uuid::Uuid::new_v4().simple());
            seen.insert(call.id.clone());
        }
        if call.function.arguments.trim().is_empty() {
            call.function.arguments = "{}".to_string();
        }
        true
    });
    completion
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_key(api_key: Option<&str>) -> Config {
        Config {
            api_key: api_key.map(str::to_string),
            base_url: "http://localhost".to_string(),
            default_model: "deepseek-flash".to_string(),
            models: vec!["deepseek-flash".to_string()],
            data_dir: std::path::PathBuf::from(".dsh-acp"),
            permission_mode: crate::config::PermissionMode::Ask,
            system_prompt: None,
            max_tokens: Some(100),
            reasoning_effort: ReasoningEffort::High,
            context_chars: 1000,
            tool_output_bytes: 1000,
            http_timeout: std::time::Duration::from_secs(1),
            mcp_connect_timeout: std::time::Duration::from_secs(1),
            disable_web: true,
            trace: false,
        }
    }

    #[tokio::test]
    async fn provider_http_bodies_fail_cleanly_over_the_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let failure = read_provider_body(
            response,
            16,
            &CancellationToken::new(),
            "test provider response",
        )
        .await
        .unwrap_err();
        assert!(!failure.retryable);
        assert!(
            crate::tools::error_reason(&failure.error).contains("16-byte limit"),
            "{:?}",
            failure.error
        );
        server.await.unwrap();
    }

    #[test]
    fn provider_tool_call_indices_are_bounded() {
        let mut completion = Completion::default();
        let event = json!({
            "choices": [{"delta": {"tool_calls": [{
                "index": u64::MAX,
                "function": {"name": "tool", "arguments": "{}"}
            }]}}]
        })
        .to_string();
        let mut events = Vec::new();
        let error = ingest_stream_data(&event, &mut completion, &mut |event| {
            events.push(event);
        })
        .unwrap_err();
        assert!(
            crate::tools::error_reason(&error).contains("128-call limit"),
            "{error:?}"
        );
        assert!(events.is_empty());
    }

    #[test]
    fn request_carries_thinking_effort() {
        let config = config_with_key(Some("key"));
        let client = DeepSeekClient::new(&config).unwrap();
        let request = client.request(
            "deepseek-flash",
            ReasoningEffort::Max,
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(request.reasoning_effort, Some("max"));
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["reasoning_effort"], "max");

        assert!(json.get("temperature").is_none());
        let request = client.request(
            "deepseek-flash",
            ReasoningEffort::None,
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(request.reasoning_effort, Some("none"));
        assert!(
            serde_json::to_value(&request)
                .unwrap()
                .get("temperature")
                .is_none()
        );

        let request = client.request(
            "deepseek-reasoner",
            ReasoningEffort::High,
            Vec::new(),
            Vec::new(),
        );
        assert!(
            serde_json::to_value(&request)
                .unwrap()
                .get("temperature")
                .is_none()
        );
    }

    #[test]
    fn a_blank_api_key_counts_as_no_key() {
        for api_key in ["", "   "] {
            let config = config_with_key(Some(api_key));
            let error = DeepSeekClient::new(&config)
                .err()
                .expect("a blank key is not a key");
            assert_eq!(error.code, agent_client_protocol::ErrorCode::AuthRequired);
            assert_eq!(
                error.data,
                Some(json!(
                    "Set DEEPSEEK_API_KEY, pass --api-key, or configure an Authorization provider header"
                ))
            );

            let headers = std::collections::HashMap::from([(
                "Authorization".to_string(),
                "Bearer provider-key".to_string(),
            )]);
            let client =
                DeepSeekClient::with_routing(&config, config.base_url.clone(), headers).unwrap();
            assert!(client.api_key.is_none(), "{api_key:?} must not be sent");
        }
    }

    #[test]
    fn parses_stream_events_and_usage() {
        let mut completion = Completion::default();
        let mut events = Vec::new();
        ingest_stream_data(
            r#"{"choices":[{"delta":{"reasoning_content":"think "}}]}"#,
            &mut completion,
            &mut |event| events.push(event),
        )
        .unwrap();
        ingest_stream_data(
            r#"{"choices":[{"delta":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#,
            &mut completion,
            &mut |event| events.push(event),
        )
        .unwrap();
        assert_eq!(completion.content, "hello");
        assert_eq!(completion.reasoning_content, "think ");
        assert_eq!(completion.usage.total_tokens, 5);
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn recognizes_standard_error_shape() {
        assert_eq!(
            extract_api_error(r#"{"error":{"message":"bad key"}}"#),
            Some("bad key".to_string())
        );
    }
}
