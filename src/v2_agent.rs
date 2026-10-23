use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use agent_client_protocol::{
    Agent, Client, ConnectTo, Error, UntypedMessage, V2ConnectionTo,
    schema::v2::{
        self, AgentCapabilities, AgentMessage, AgentThought, AuthMethod, AuthMethodAgent,
        AvailableCommand, CompactionStatus, CompactionUpdate, ContentBlock, ContentChunk, Diff,
        DiffChange, DiffFileType, DisableProviderRequest, DisableProviderResponse, IdleStateUpdate,
        Implementation, InitializeRequest, InitializeResponse, ListProvidersRequest,
        ListProvidersResponse, LlmProtocol, LoginAuthRequest, LoginAuthResponse, LogoutAuthRequest,
        LogoutAuthResponse, Notice, NoticeSeverity, PermissionOption, PermissionOptionKind,
        PlanEntry, PlanEntryPriority, PlanEntryStatus, PlanUpdate, PlanUpdateContent,
        PromptRequest, PromptResponse, ProviderCurrentConfig, ProviderId, ProviderInfo,
        ProvidersCapabilities, ReplayFrom, RequestPermissionOutcome, RequestPermissionRequest,
        RequiresActionStateUpdate, RunningStateUpdate, SessionUpdate, SetProviderRequest,
        SetProviderResponse, StateUpdate, StopReason, Terminal, TerminalExitStatus, TerminalOutput,
        TerminalOutputChunk, TerminalUpdate, TextContent, ToolCallContent, ToolCallContentChunk,
        ToolCallStatus, ToolCallUpdate, UpdateSessionNotification,
    },
};
use base64::Engine as _;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::Harness,
    deepseek::{Completion, StreamEvent},
    model::{
        ChatMessage, ContentPart, ImageUrl, MessageContent, PendingUserMessage,
        PersistedCompaction, PersistedDiff, Role, SessionRecord, SessionRuntime, TokenUsage,
        ToolOutcomeStatus, ensure_session_unique_tool_call_ids,
    },
    session::session_title,
    tools::{
        ToolAccess, ToolResult, built_in_definitions, git_patch, persisted_diff,
        persisted_tool_outcome, result_status, structured_content,
    },
};

const PAGE_SIZE: usize = 100;
const MAX_MODEL_TOOLS: usize = 128;
const CONTEXT_WINDOW: u64 = 128 * 1024;
const STEERING_METHOD: &str = "_session/steering";

#[derive(Clone)]
struct V2Harness {
    base: Harness,
    capabilities: Arc<RwLock<v2::ClientCapabilities>>,
    initialized: Arc<AtomicBool>,
}

pub(crate) fn component(harness: Harness) -> impl ConnectTo<Client> + 'static {
    let harness = V2Harness {
        base: harness,
        capabilities: Arc::new(RwLock::new(v2::ClientCapabilities::default())),
        initialized: Arc::new(AtomicBool::new(false)),
    };
    Agent
        .v2()
        .name("dsh-acp-v2")
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: InitializeRequest,
                            responder: agent_client_protocol::Responder<InitializeResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().initialize(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: LoginAuthRequest,
                            responder: agent_client_protocol::Responder<LoginAuthResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().login(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: LogoutAuthRequest,
                            responder: agent_client_protocol::Responder<LogoutAuthResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().logout(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: ListProvidersRequest,
                            responder: agent_client_protocol::Responder<ListProvidersResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().list_providers(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: SetProviderRequest,
                            responder: agent_client_protocol::Responder<SetProviderResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().set_provider(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: DisableProviderRequest,
                            responder: agent_client_protocol::Responder<DisableProviderResponse>,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().disable_provider(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::NewSessionRequest,
                            responder: agent_client_protocol::Responder<v2::NewSessionResponse>,
                            connection: V2ConnectionTo<Client>| {
                    harness
                        .clone()
                        .new_session(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::ListSessionsRequest,
                            responder: agent_client_protocol::Responder<
                    v2::ListSessionsResponse,
                >,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().list_sessions(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::ResumeSessionRequest,
                            responder: agent_client_protocol::Responder<
                    v2::ResumeSessionResponse,
                >,
                            connection: V2ConnectionTo<Client>| {
                    harness
                        .clone()
                        .resume_session(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::ForkSessionRequest,
                            responder: agent_client_protocol::Responder<v2::ForkSessionResponse>,
                            connection: V2ConnectionTo<Client>| {
                    harness
                        .clone()
                        .fork_session(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::DeleteSessionRequest,
                            responder: agent_client_protocol::Responder<
                    v2::DeleteSessionResponse,
                >,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().delete_session(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::CloseSessionRequest,
                            responder: agent_client_protocol::Responder<
                    v2::CloseSessionResponse,
                >,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().close_session(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: v2::SetSessionConfigOptionRequest,
                            responder: agent_client_protocol::Responder<
                    v2::SetSessionConfigOptionResponse,
                >,
                            connection: V2ConnectionTo<Client>| {
                    harness
                        .clone()
                        .set_config(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: PromptRequest,
                            responder: agent_client_protocol::Responder<PromptResponse>,
                            connection: V2ConnectionTo<Client>| {
                    harness.clone().prompt(request, responder, connection).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: UntypedMessage,
                            responder: agent_client_protocol::Responder<Value>,
                            connection: V2ConnectionTo<Client>| {
                    if request.method != STEERING_METHOD {
                        return responder.respond_with_error(Error::method_not_found());
                    }
                    harness
                        .clone()
                        .steering(request.params, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let harness = harness.clone();
                async move |notification: v2::CancelSessionNotification,
                            _connection: V2ConnectionTo<Client>| {
                    harness.clone().cancel(notification).await;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
}

impl V2Harness {
    async fn initialize(
        self,
        request: InitializeRequest,
        responder: agent_client_protocol::Responder<InitializeResponse>,
    ) -> agent_client_protocol::Result<()> {
        *self.capabilities.write().await = request.capabilities.clone();
        self.initialized.store(true, Ordering::Release);
        let mut prompt_capabilities = v2::PromptCapabilities::new()
            .embedded_context(v2::PromptEmbeddedContextCapabilities::new());
        if self
            .base
            .config
            .models
            .iter()
            .any(|model| crate::config::Config::model_supports_images(model))
        {
            prompt_capabilities = prompt_capabilities.image(v2::PromptImageCapabilities::new());
        }
        let session_capabilities = v2::SessionCapabilities::new()
            .prompt(prompt_capabilities)
            .mcp(
                v2::McpCapabilities::new()
                    .stdio(v2::McpStdioCapabilities::new())
                    .http(v2::McpHttpCapabilities::new()),
            )
            .delete(v2::SessionDeleteCapabilities::new())
            .additional_directories(v2::SessionAdditionalDirectoriesCapabilities::new())
            .fork(v2::SessionForkCapabilities::new())
            .system_prompt(v2::SessionSystemPromptCapabilities::new());
        let capabilities = AgentCapabilities::new()
            .session(session_capabilities)
            .providers(ProvidersCapabilities::new());
        responder.respond(
            InitializeResponse::new(
                request.protocol_version,
                Implementation::new("dsh-acp", env!("CARGO_PKG_VERSION")).title("DeepSeek Harness"),
            )
            .capabilities(capabilities)
            .auth_methods(vec![AuthMethod::Agent(
                AuthMethodAgent::new("deepseek-api-key", "DeepSeek API key").description(
                    "Uses DEEPSEEK_API_KEY, --api-key, or an Authorization provider header.",
                ),
            )])
            .meta(Some(serde_json::Map::from_iter([(
                "steering".to_string(),
                json!({"supported": true, "idleBehaviors": ["promptRequired"]}),
            )]))),
        )
    }

    async fn login(
        self,
        request: LoginAuthRequest,
        responder: agent_client_protocol::Responder<LoginAuthResponse>,
    ) -> agent_client_protocol::Result<()> {
        if request.method_id.to_string() != "deepseek-api-key" {
            return responder
                .respond_with_error(Error::invalid_params().data("unknown authentication method"));
        }
        let routing = self.base.provider.read().await.clone();
        let has_provider_auth = routing
            .headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"));
        let has_api_key = self
            .base
            .config
            .api_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty());
        if has_api_key || has_provider_auth {
            self.base.provider.write().await.enabled = true;
            responder.respond(LoginAuthResponse::new())
        } else {
            responder.respond_with_error(Error::auth_required().data(
                "Configure DEEPSEEK_API_KEY, --api-key, or a provider Authorization header first",
            ))
        }
    }

    async fn logout(
        self,
        _request: LogoutAuthRequest,
        responder: agent_client_protocol::Responder<LogoutAuthResponse>,
    ) -> agent_client_protocol::Result<()> {
        self.base.provider.write().await.enabled = false;
        responder.respond(LogoutAuthResponse::new())
    }

    async fn list_providers(
        self,
        _request: ListProvidersRequest,
        responder: agent_client_protocol::Responder<ListProvidersResponse>,
    ) -> agent_client_protocol::Result<()> {
        let routing = self.base.provider.read().await;
        let current = routing
            .enabled
            .then(|| ProviderCurrentConfig::new(LlmProtocol::OpenAi, routing.base_url.clone()));
        responder.respond(ListProvidersResponse::new(vec![ProviderInfo::new(
            ProviderId::new("deepseek"),
            vec![LlmProtocol::OpenAi],
            false,
            current,
        )]))
    }

    async fn set_provider(
        self,
        request: SetProviderRequest,
        responder: agent_client_protocol::Responder<SetProviderResponse>,
    ) -> agent_client_protocol::Result<()> {
        if request.provider_id.to_string() != "deepseek" {
            return responder.respond_with_error(
                Error::invalid_params().data("only the deepseek provider is supported"),
            );
        }
        if !matches!(&request.api_type, LlmProtocol::OpenAi) {
            return responder.respond_with_error(
                Error::invalid_params().data("DeepSeek requires the openai protocol"),
            );
        }
        if reqwest::Url::parse(&request.base_url).is_err() {
            return responder
                .respond_with_error(Error::invalid_params().data("invalid provider base URL"));
        }
        *self.base.provider.write().await = crate::agent::ProviderRouting {
            enabled: true,
            base_url: request.base_url,
            headers: request.headers,
        };
        responder.respond(SetProviderResponse::new())
    }

    async fn disable_provider(
        self,
        request: DisableProviderRequest,
        responder: agent_client_protocol::Responder<DisableProviderResponse>,
    ) -> agent_client_protocol::Result<()> {
        if request.provider_id.to_string() != "deepseek" {
            return responder.respond_with_error(
                Error::invalid_params().data("only the deepseek provider is supported"),
            );
        }
        self.base.provider.write().await.enabled = false;
        responder.respond(DisableProviderResponse::new())
    }

    async fn new_session(
        self,
        request: v2::NewSessionRequest,
        responder: agent_client_protocol::Responder<v2::NewSessionResponse>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        if !self.initialized.load(Ordering::Acquire) {
            return responder.respond_with_error(Error::invalid_request().data("initialize first"));
        }
        let system_prompt = match v2_system_prompt_text(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt,
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self
            .base
            .store
            .create(
                request.cwd.into_inner(),
                absolute_paths(request.additional_directories),
                system_prompt,
                None,
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            let _ = self.base.store.delete(&runtime.snapshot().await.id).await;
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        responder.respond(
            v2::NewSessionResponse::new(record.id.clone())
                .config_options(self.config_options(&record))
                .available_commands(available_commands()),
        )?;
        send_update(
            &connection,
            &v2_session_id(&record.id),
            SessionUpdate::StateUpdate(StateUpdate::Idle(IdleStateUpdate::new())),
        )
    }

    async fn list_sessions(
        self,
        request: v2::ListSessionsRequest,
        responder: agent_client_protocol::Responder<v2::ListSessionsResponse>,
    ) -> agent_client_protocol::Result<()> {
        let offset = match request.cursor.as_ref() {
            Some(cursor) => match cursor.as_ref().parse::<usize>() {
                Ok(offset) => offset,
                Err(_) => {
                    return responder.respond_with_error(
                        Error::invalid_params().data("invalid pagination cursor"),
                    );
                }
            },
            None => 0,
        };
        let records = match self
            .base
            .store
            .list(request.cwd.as_ref().map(v2::AbsolutePath::as_ref))
            .await
        {
            Ok(records) => records,
            Err(error) => return responder.respond_with_error(error),
        };
        let total = records.len();
        let sessions = records
            .into_iter()
            .skip(offset)
            .take(PAGE_SIZE)
            .map(v2_session_info)
            .collect::<Vec<_>>();
        let next = offset.saturating_add(sessions.len());
        responder.respond(
            v2::ListSessionsResponse::new(sessions)
                .next_cursor((next < total).then(|| v2::SessionListCursor::new(next.to_string()))),
        )
    }

    async fn resume_session(
        self,
        request: v2::ResumeSessionRequest,
        responder: agent_client_protocol::Responder<v2::ResumeSessionResponse>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        if matches!(request.replay_from.as_ref(), Some(ReplayFrom::Other(_))) {
            return responder
                .respond_with_error(Error::invalid_params().data("unsupported replay cursor"));
        }
        let runtime = match self.base.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let additional_directories = absolute_paths(request.additional_directories.clone());
        if let Err(error) = crate::session::validate_roots(&request.cwd.0, &additional_directories)
        {
            return responder.respond_with_error(error);
        }
        {
            let mut state = runtime.state.lock().await;
            if state.record.cwd != request.cwd.0 {
                return responder.respond_with_error(Error::invalid_params().data(format!(
                    "session cwd mismatch: stored {}, requested {}",
                    state.record.cwd.display(),
                    request.cwd.0.display()
                )));
            }
            state.record.additional_directories = additional_directories;
            state.record.touch();
        }
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            return responder.respond_with_error(error);
        }
        if let Err(error) = self.base.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        if request.replay_from.is_some()
            && let Err(error) = replay_history(&connection, &record)
        {
            return responder.respond_with_error(error);
        }
        {
            let state = runtime.state.lock().await;
            let current = if state.requires_action.load(Ordering::Acquire) {
                StateUpdate::RequiresAction(RequiresActionStateUpdate::new())
            } else if state.active_prompt.load(Ordering::Acquire) {
                StateUpdate::Running(RunningStateUpdate::new())
            } else {
                StateUpdate::Idle(IdleStateUpdate::new())
            };
            send_update(
                &connection,
                &v2_session_id(&record.id),
                SessionUpdate::StateUpdate(current),
            )?;
        }
        responder.respond(
            v2::ResumeSessionResponse::new()
                .config_options(self.config_options(&record))
                .available_commands(available_commands()),
        )
    }

    async fn fork_session(
        self,
        request: v2::ForkSessionRequest,
        responder: agent_client_protocol::Responder<v2::ForkSessionResponse>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let source_runtime = match self.base.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        if source_runtime
            .state
            .lock()
            .await
            .active_prompt
            .load(Ordering::Acquire)
        {
            return responder.respond_with_error(
                Error::invalid_request().data("cannot fork a session with active work"),
            );
        }
        let source = source_runtime.snapshot().await;
        if source.cwd != request.cwd.0 {
            return responder.respond_with_error(Error::invalid_params().data(format!(
                "session cwd mismatch: stored {}, requested {}",
                source.cwd.display(),
                request.cwd.0.display()
            )));
        }
        let system_prompt = match v2_system_prompt_text(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt.or(source.system_prompt.clone()),
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self
            .base
            .store
            .create(
                request.cwd.into_inner(),
                absolute_paths(request.additional_directories),
                system_prompt,
                Some(source.id.clone()),
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        {
            let mut state = runtime.state.lock().await;
            state.record.messages = source.messages.clone();
            state.record.prompt_blocks = source.prompt_blocks.clone();
            state.record.message_ids = source.message_ids.clone();
            state.record.pending_steering = source.pending_steering.clone();
            state.record.tool_outcomes = source.tool_outcomes.clone();
            state.record.compactions = source.compactions.clone();
            state.record.config = source.config;
            state.record.title = source.title;
            state.record.usage = source.usage;
            state.record.context_used = source.context_used;
            let prompt = self.base.store.effective_system_prompt(&state.record);
            state.record.install_system_prompt(prompt);
            state.record.touch();
        }
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            return responder.respond_with_error(error);
        }
        if let Err(error) = self.base.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        responder.respond(
            v2::ForkSessionResponse::new(record.id.clone())
                .config_options(self.config_options(&record))
                .available_commands(available_commands()),
        )?;
        send_update(
            &connection,
            &v2_session_id(&record.id),
            SessionUpdate::StateUpdate(StateUpdate::Idle(IdleStateUpdate::new())),
        )
    }

    async fn delete_session(
        self,
        request: v2::DeleteSessionRequest,
        responder: agent_client_protocol::Responder<v2::DeleteSessionResponse>,
    ) -> agent_client_protocol::Result<()> {
        match self
            .base
            .store
            .delete(&request.session_id.to_string())
            .await
        {
            Ok(()) => responder.respond(v2::DeleteSessionResponse::new()),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn close_session(
        self,
        request: v2::CloseSessionRequest,
        responder: agent_client_protocol::Responder<v2::CloseSessionResponse>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.base.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        {
            let _lifecycle = runtime.lifecycle.lock().await;
            runtime.closed.store(true, Ordering::Release);
        }
        let (active, notify, cancellation) = {
            let state = runtime.state.lock().await;
            (
                state.active_prompt.clone(),
                state.activity_notify.clone(),
                state.cancellation.clone(),
            )
        };
        cancellation.cancel();
        loop {
            let notified = notify.notified();
            if !active.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        match self.base.store.close(&request.session_id.to_string()).await {
            Ok(()) => responder.respond(v2::CloseSessionResponse::new()),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn set_config(
        self,
        request: v2::SetSessionConfigOptionRequest,
        responder: agent_client_protocol::Responder<v2::SetSessionConfigOptionResponse>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.base.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        {
            let mut state = runtime.state.lock().await;
            if let Err(error) = apply_config(
                &mut state.record,
                request.config_id.to_string().as_str(),
                &request.value,
                &self.base.config.models,
            ) {
                return responder.respond_with_error(error);
            }
            state.record.touch();
        }
        if let Err(error) = self.base.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        let options = self.config_options(&record);
        send_update(
            &connection,
            &v2_session_id(&record.id),
            SessionUpdate::ConfigOptionUpdate(v2::ConfigOptionUpdate::new(options.clone())),
        )?;
        responder.respond(v2::SetSessionConfigOptionResponse::new(options))
    }

    async fn prompt(
        self,
        request: PromptRequest,
        responder: agent_client_protocol::Responder<PromptResponse>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.base.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let _lifecycle = runtime.lifecycle.lock().await;
        if runtime.closed.load(Ordering::Acquire) {
            return responder.respond_with_error(Error::request_cancelled());
        }
        let prompt = match content_blocks_to_prompt(&request.prompt) {
            Ok(prompt) if !prompt.text.trim().is_empty() || prompt.has_images => prompt,
            Ok(_) => {
                return responder
                    .respond_with_error(Error::invalid_params().data("prompt is empty"));
            }
            Err(error) => return responder.respond_with_error(error),
        };
        let model = runtime.snapshot().await.config.model;
        if prompt.has_images
            && !prompt.text.trim().starts_with('/')
            && !crate::config::Config::model_supports_images(&model)
        {
            return responder.respond_with_error(Error::invalid_params().data(format!(
                "model {model} does not support image prompts; select deepseek-flash"
            )));
        }
        let cancellation = CancellationToken::new();
        let turn = runtime.turn_lock.lock().await;
        let (message_id, message_index, previous_title, starts_work, prompt_seq) = {
            let mut state = runtime.state.lock().await;
            let starts_work = !state.active_prompt.load(Ordering::Acquire);
            if starts_work {
                state.active_prompt.store(true, Ordering::Release);
                state.cancellation = cancellation.clone();
            }
            state.prompt_seq = state.prompt_seq.saturating_add(1);
            let prompt_seq = state.prompt_seq;
            let message_id = message_id_for(
                &state.record.id,
                state.record.messages.len() + state.record.pending_steering.len(),
            );
            let message_index = starts_work.then_some(state.record.messages.len());
            let previous_title = state.record.title.clone();
            let blocks = if let Ok(blocks) = serde_json::to_value(&request.prompt)
                && let Some(blocks) = blocks.as_array()
            {
                blocks.clone()
            } else {
                Vec::new()
            };
            if let Some(message_index) = message_index {
                state
                    .record
                    .messages
                    .push(ChatMessage::user_content(prompt.content.clone()));
                state
                    .record
                    .prompt_blocks
                    .insert(message_index.to_string(), blocks);
                state
                    .record
                    .message_ids
                    .insert(message_index.to_string(), message_id.clone());
            } else {
                state.record.pending_steering.push(PendingUserMessage {
                    message_id: message_id.clone(),
                    content: prompt.content.clone(),
                    blocks,
                });
            }
            state.record.touch();
            if state.record.title.is_none() {
                state.record.title = session_title(&state.record);
            }
            (
                message_id,
                message_index,
                previous_title,
                starts_work,
                prompt_seq,
            )
        };
        if let Err(error) = self.base.store.save(&runtime).await {
            let mut state = runtime.state.lock().await;
            if let Some(message_index) = message_index {
                if state.record.messages.len() > message_index {
                    state.record.messages.remove(message_index);
                }
                state
                    .record
                    .prompt_blocks
                    .remove(&message_index.to_string());
                state.record.message_ids.remove(&message_index.to_string());
            } else {
                state.record.pending_steering.pop();
            }
            state.record.title = previous_title;
            state.prompt_seq = state.prompt_seq.saturating_sub(1);
            if starts_work {
                state.active_prompt.store(false, Ordering::Release);
                state.activity_notify.notify_waiters();
            }
            return responder.respond_with_error(error);
        }
        connection.send_notification(UpdateSessionNotification::new(
            request.session_id.clone(),
            SessionUpdate::UserMessage(
                v2::UserMessage::new(message_id.clone()).content(request.prompt.clone()),
            ),
        ))?;
        responder.respond(PromptResponse::new(message_id))?;
        drop(turn);
        drop(_lifecycle);

        if !starts_work {
            return Ok(());
        }

        let prompt_connection = connection.clone();
        let capabilities = self.capabilities.read().await.clone();
        let work_runtime = runtime.clone();
        let work_session_id = request.session_id.clone();
        let spawn_result = connection.spawn(async move {
            self.run_prompt_work(
                work_runtime,
                work_session_id,
                prompt.text,
                prompt_seq,
                capabilities,
                prompt_connection,
                cancellation,
            )
            .await;
            Ok(())
        });
        if let Err(error) = spawn_result {
            let state = runtime.state.lock().await;
            state.active_prompt.store(false, Ordering::Release);
            state.activity_notify.notify_waiters();
            let _ = send_update(
                &connection,
                &request.session_id,
                SessionUpdate::StateUpdate(StateUpdate::Idle(IdleStateUpdate::new().stop_reason(
                    Some(StopReason::Error(
                        v2::ErrorStopReason::new().error(Some(v2_error_from(error))),
                    )),
                ))),
            );
        }
        Ok(())
    }

    async fn steering(
        self,
        params: Value,
        responder: agent_client_protocol::Responder<Value>,
        connection: V2ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|session_id| !session_id.is_empty())
            .ok_or_else(|| {
                Error::invalid_params().data("steering requires a non-empty sessionId")
            })?;
        let prompt = params
            .get("prompt")
            .cloned()
            .ok_or_else(|| Error::invalid_params().data("steering requires prompt content"))?;
        let prompt = serde_json::from_value::<Vec<ContentBlock>>(prompt)
            .map_err(|error| Error::invalid_params().data(error.to_string()))?;
        if prompt.is_empty() {
            return responder.respond_with_error(
                Error::invalid_params().data("steering prompt must not be empty"),
            );
        }
        let idle_behavior = params
            .pointer("/_meta/steering/idleBehavior")
            .and_then(Value::as_str);
        if let Some(idle_behavior) = idle_behavior
            && idle_behavior != "promptRequired"
        {
            return responder.respond_with_error(
                Error::invalid_params().data("unsupported steering idleBehavior"),
            );
        }
        let session_id = v2::SessionId::new(session_id.to_string());
        let runtime = match self.base.store.get(&session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let _lifecycle = runtime.lifecycle.lock().await;
        if runtime.closed.load(Ordering::Acquire) {
            return responder
                .respond_with_error(Error::invalid_request().data("session is closing"));
        }
        let turn = runtime.turn_lock.lock().await;
        let is_active = runtime
            .state
            .lock()
            .await
            .active_prompt
            .load(Ordering::Acquire);
        if !is_active && idle_behavior == Some("promptRequired") {
            return responder.respond(json!({
                "outcome": "promptRequired",
                "reason": "noRunningTurn"
            }));
        }
        let payload = match content_blocks_to_prompt(&prompt) {
            Ok(payload) if !payload.text.trim().is_empty() || payload.has_images => payload,
            Ok(_) => {
                return responder
                    .respond_with_error(Error::invalid_params().data("steering prompt is empty"));
            }
            Err(error) => return responder.respond_with_error(error),
        };
        let model = runtime.snapshot().await.config.model;
        if payload.has_images
            && !payload.text.trim().starts_with('/')
            && !crate::config::Config::model_supports_images(&model)
        {
            return responder.respond_with_error(Error::invalid_params().data(format!(
                "model {model} does not support image prompts; select deepseek-flash"
            )));
        }
        let blocks = serde_json::to_value(&prompt)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        if is_active {
            let (message_id, previous_index, previous_seq) = {
                let mut state = runtime.state.lock().await;
                let previous_index = state.record.pending_steering.len();
                let previous_seq = state.prompt_seq;
                state.prompt_seq = state.prompt_seq.saturating_add(1);
                let message_id = message_id_for(
                    &state.record.id,
                    state.record.messages.len() + state.record.pending_steering.len(),
                );
                state.record.pending_steering.push(PendingUserMessage {
                    message_id: message_id.clone(),
                    content: payload.content,
                    blocks,
                });
                state.record.touch();
                (message_id, previous_index, previous_seq)
            };
            if let Err(error) = self.base.store.save(&runtime).await {
                let mut state = runtime.state.lock().await;
                state.record.pending_steering.truncate(previous_index);
                state.prompt_seq = previous_seq;
                return responder.respond_with_error(error);
            }
            send_update(
                &connection,
                &session_id,
                SessionUpdate::UserMessage(v2::UserMessage::new(message_id).content(prompt)),
            )?;
            return responder.respond(json!({"outcome": "injected"}));
        }

        let cancellation = CancellationToken::new();
        let (message_id, message_index, previous_title, prompt_seq) = {
            let mut state = runtime.state.lock().await;
            state.active_prompt.store(true, Ordering::Release);
            state.cancellation = cancellation.clone();
            state.prompt_seq = state.prompt_seq.saturating_add(1);
            let prompt_seq = state.prompt_seq;
            let message_index = state.record.messages.len();
            let previous_title = state.record.title.clone();
            let message_id = message_id_for(&state.record.id, message_index);
            state
                .record
                .messages
                .push(ChatMessage::user_content(payload.content));
            state
                .record
                .prompt_blocks
                .insert(message_index.to_string(), blocks);
            state
                .record
                .message_ids
                .insert(message_index.to_string(), message_id.clone());
            state.record.touch();
            if state.record.title.is_none() {
                state.record.title = session_title(&state.record);
            }
            (message_id, message_index, previous_title, prompt_seq)
        };
        if let Err(error) = self.base.store.save(&runtime).await {
            let mut state = runtime.state.lock().await;
            state.record.messages.remove(message_index);
            state
                .record
                .prompt_blocks
                .remove(&message_index.to_string());
            state.record.message_ids.remove(&message_index.to_string());
            state.record.title = previous_title;
            state.prompt_seq = prompt_seq.saturating_sub(1);
            state.active_prompt.store(false, Ordering::Release);
            state.activity_notify.notify_waiters();
            return responder.respond_with_error(error);
        }
        send_update(
            &connection,
            &session_id,
            SessionUpdate::UserMessage(v2::UserMessage::new(message_id).content(prompt)),
        )?;
        send_update(
            &connection,
            &session_id,
            SessionUpdate::StateUpdate(StateUpdate::Running(RunningStateUpdate::new())),
        )?;
        let work_connection = connection.clone();
        let capabilities = self.capabilities.read().await.clone();
        let work_runtime = runtime.clone();
        let work_session_id = session_id.clone();
        let spawn_result = connection.spawn(async move {
            self.run_prompt_work(
                work_runtime,
                work_session_id,
                payload.text,
                prompt_seq,
                capabilities,
                work_connection,
                cancellation,
            )
            .await;
            Ok(())
        });
        drop(turn);
        drop(_lifecycle);
        if let Err(error) = spawn_result {
            let state = runtime.state.lock().await;
            state.active_prompt.store(false, Ordering::Release);
            state.activity_notify.notify_waiters();
            let _ = send_update(
                &connection,
                &session_id,
                SessionUpdate::StateUpdate(StateUpdate::Idle(IdleStateUpdate::new().stop_reason(
                    Some(StopReason::Error(
                        v2::ErrorStopReason::new().error(Some(v2_error_from(error))),
                    )),
                ))),
            );
            return responder.respond(json!({"outcome": "failed"}));
        }
        responder.respond(json!({"outcome": "startedNewTurn"}))
    }

    async fn cancel(&self, notification: v2::CancelSessionNotification) {
        if let Ok(runtime) = self
            .base
            .store
            .get(&notification.session_id.to_string())
            .await
        {
            runtime.state.lock().await.cancellation.cancel();
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_prompt_work(
        self,
        runtime: SessionRuntime,
        session_id: v2::SessionId,
        prompt_text: String,
        mut last_request_seq: u64,
        capabilities: v2::ClientCapabilities,
        connection: V2ConnectionTo<Client>,
        cancellation: CancellationToken,
    ) {
        let record = {
            let state = runtime.state.lock().await;
            state.requires_action.store(false, Ordering::Release);
            let record = state.record.clone();
            send_update(
                &connection,
                &session_id,
                SessionUpdate::StateUpdate(StateUpdate::Running(RunningStateUpdate::new())),
            )
            .ok();
            record
        };
        let _ = send_update(
            &connection,
            &session_id,
            SessionUpdate::SessionInfoUpdate(
                v2::SessionInfoUpdate::new().title(record.title.clone()),
            ),
        );
        let initial_usage = runtime.snapshot().await.usage;
        let mut usage = initial_usage;
        let mut allow_command = true;
        loop {
            let result = self
                .run_model_loop(
                    &runtime,
                    &session_id,
                    &prompt_text,
                    allow_command,
                    &capabilities,
                    &connection,
                    &cancellation,
                    usage.clone(),
                )
                .await;
            allow_command = false;
            let (stop_reason, next_usage, request_seq) = match result {
                Ok(result) => result,
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => (
                    StopReason::Cancelled,
                    runtime.snapshot().await.usage,
                    last_request_seq,
                ),
                Err(error) => (
                    StopReason::Error(v2::ErrorStopReason::new().error(Some(v2_error_from(error)))),
                    runtime.snapshot().await.usage,
                    last_request_seq,
                ),
            };
            last_request_seq = request_seq;
            usage = next_usage;
            let (continue_for_steering, flushed_steering) = {
                let _turn = runtime.turn_lock.lock().await;
                let mut state = runtime.state.lock().await;
                let flushed_steering = !state.record.pending_steering.is_empty();
                if flushed_steering {
                    state.record.flush_pending_steering();
                    state.record.touch();
                }
                let pending_steering = state.prompt_seq > last_request_seq;
                if matches!(&stop_reason, StopReason::EndTurn) && pending_steering {
                    (true, flushed_steering)
                } else {
                    let _ = send_update(
                        &connection,
                        &session_id,
                        SessionUpdate::StateUpdate(StateUpdate::Idle(
                            IdleStateUpdate::new()
                                .stop_reason(Some(stop_reason))
                                .usage(Some(v2_usage(usage.clone()))),
                        )),
                    );
                    state.active_prompt.store(false, Ordering::Release);
                    state.requires_action.store(false, Ordering::Release);
                    state.activity_notify.notify_waiters();
                    (false, flushed_steering)
                }
            };
            if flushed_steering && let Err(error) = self.base.store.save(&runtime).await {
                tracing::warn!(?error, "failed to persist consumed steering prompt");
            }
            if !continue_for_steering {
                break;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_model_loop(
        &self,
        runtime: &SessionRuntime,
        session_id: &v2::SessionId,
        prompt: &str,
        allow_command: bool,
        capabilities: &v2::ClientCapabilities,
        connection: &V2ConnectionTo<Client>,
        cancellation: &CancellationToken,
        mut usage: TokenUsage,
    ) -> Result<(StopReason, TokenUsage, u64), Error> {
        if allow_command && prompt.trim().starts_with('/') {
            let request_seq = runtime.state.lock().await.prompt_seq;
            let before = runtime.snapshot().await;
            let response = self.command(runtime, prompt).await?;
            {
                let mut state = runtime.state.lock().await;
                state
                    .record
                    .messages
                    .push(ChatMessage::assistant(Some(response.clone()), None));
                state.record.touch();
            }
            self.base.store.save(runtime).await?;
            send_agent_text(connection, session_id, &response)?;
            let after = runtime.snapshot().await;
            if before.config.model != after.config.model
                || before.config.mode != after.config.mode
                || before.config.permission_mode != after.config.permission_mode
                || before.config.network != after.config.network
            {
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::ConfigOptionUpdate(v2::ConfigOptionUpdate::new(
                        self.config_options(&after),
                    )),
                )?;
            }
            return Ok((
                StopReason::EndTurn,
                runtime.snapshot().await.usage,
                request_seq,
            ));
        }
        let client = self.base.deepseek_client().await?;
        let supports_elicitation = capabilities
            .elicitation
            .as_ref()
            .is_some_and(|capabilities| {
                capabilities.supports_form() || capabilities.supports_url()
            });
        let mut compaction_reported = false;
        for round in 0..self.base.config.max_tool_rounds {
            {
                let mut state = runtime.state.lock().await;
                if !state.record.pending_steering.is_empty() {
                    state.record.flush_pending_steering();
                    state.record.touch();
                    drop(state);
                    self.base.store.save(runtime).await?;
                }
            }
            if cancellation.is_cancelled() {
                return Ok((
                    StopReason::Cancelled,
                    usage,
                    runtime.state.lock().await.prompt_seq,
                ));
            }
            let (record, request_seq) = {
                let state = runtime.state.lock().await;
                (state.record.clone(), state.prompt_seq)
            };
            let mut tools = built_in_definitions(&self.base.config);
            if supports_elicitation {
                tools.push(crate::tools::ask_user_definition());
            }
            {
                let mut state = runtime.state.lock().await;
                if let Some(mcp) = state.mcp.as_mut() {
                    if let Err(error) = mcp.refresh(cancellation).await {
                        tracing::warn!(?error, "failed to refresh MCP tools");
                        let _ = send_update(
                            connection,
                            session_id,
                            SessionUpdate::Notice(
                                Notice::new(NoticeSeverity::Warning, "MCP tool refresh failed")
                                    .description(Some(error.message)),
                            ),
                        );
                    }
                    tools.extend(mcp.definitions());
                }
            }
            tools.truncate(MAX_MODEL_TOOLS);
            let mut context = record.context_messages(self.base.config.context_chars);
            if record.config.mode == "plan" {
                crate::tools::restrict_to_plan_mode(&mut tools, &mut context);
            }
            let retained_non_system = context
                .iter()
                .filter(|message| message.role != Role::System)
                .count();
            let original_non_system = record
                .messages
                .iter()
                .filter(|message| message.role != Role::System)
                .count();
            if !compaction_reported && retained_non_system < original_non_system {
                let compaction_id = format!("cmp-{}", uuid::Uuid::new_v4().simple());
                {
                    let mut state = runtime.state.lock().await;
                    state.record.compactions.push(PersistedCompaction {
                        id: compaction_id.clone(),
                        status: "completed".to_string(),
                    });
                    state.record.touch();
                }
                self.base.store.save(runtime).await?;
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::CompactionUpdate(CompactionUpdate::new(
                        compaction_id,
                        CompactionStatus::Completed,
                    )),
                )?;
                compaction_reported = true;
            }
            let request = client.request(record.config.model.clone(), context, tools);
            let answer_id = message_id_for(&record.id, record.messages.len());
            let thought_id = format!("{answer_id}-thought");
            let mut on_event = |event: StreamEvent| match event {
                StreamEvent::Text(text) => {
                    let _ = send_chunk(connection, session_id, &answer_id, text, false);
                }
                StreamEvent::Reasoning(text) => {
                    let _ = send_chunk(connection, session_id, &thought_id, text, true);
                }
                StreamEvent::Usage(_) => {}
            };
            let mut completion = client
                .stream_chat(request, cancellation, &mut on_event)
                .await?;
            ensure_session_unique_tool_call_ids(
                &runtime.snapshot().await,
                &mut completion.tool_calls,
            );
            usage.saturating_add(&completion.usage);
            self.persist_completion(runtime, &completion, usage.clone())
                .await?;
            send_update(
                connection,
                session_id,
                SessionUpdate::UsageUpdate(v2::UsageUpdate::new(
                    runtime.snapshot().await.context_used,
                    CONTEXT_WINDOW,
                )),
            )?;
            if completion.tool_calls.is_empty() {
                {
                    let mut state = runtime.state.lock().await;
                    if !state.record.pending_steering.is_empty() {
                        state.record.flush_pending_steering();
                        state.record.touch();
                        drop(state);
                        self.base.store.save(runtime).await?;
                    }
                }
                let reason = match completion.finish_reason.as_deref() {
                    Some("length") => StopReason::MaxTokens,
                    Some("content_filter") => StopReason::Refusal,
                    _ => StopReason::EndTurn,
                };
                return Ok((reason, usage, request_seq));
            }
            if round + 1 == self.base.config.max_tool_rounds {
                {
                    let mut state = runtime.state.lock().await;
                    state.record.flush_pending_steering();
                    state.record.touch();
                }
                self.base.store.save(runtime).await?;
                self.append_unfinished_tools(
                    runtime,
                    session_id,
                    connection,
                    &completion.tool_calls,
                    0,
                    ToolOutcomeStatus::Cancelled,
                )
                .await?;
                return Ok((StopReason::MaxTurnRequests, usage, request_seq));
            }
            for (index, call) in completion.tool_calls.iter().enumerate() {
                if cancellation.is_cancelled() {
                    self.append_unfinished_tools(
                        runtime,
                        session_id,
                        connection,
                        &completion.tool_calls,
                        index,
                        ToolOutcomeStatus::Cancelled,
                    )
                    .await?;
                    return Ok((StopReason::Cancelled, usage, request_seq));
                }
                let result = match self
                    .execute_tool(
                        runtime,
                        session_id,
                        call,
                        capabilities,
                        connection,
                        cancellation,
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(error)
                        if error.code == agent_client_protocol::ErrorCode::RequestCancelled =>
                    {
                        self.append_unfinished_tools(
                            runtime,
                            session_id,
                            connection,
                            &completion.tool_calls,
                            index,
                            ToolOutcomeStatus::Cancelled,
                        )
                        .await?;
                        return Ok((StopReason::Cancelled, usage, request_seq));
                    }
                    Err(error) => {
                        self.append_unfinished_tools(
                            runtime,
                            session_id,
                            connection,
                            &completion.tool_calls,
                            index,
                            ToolOutcomeStatus::Failed,
                        )
                        .await?;
                        return Err(error);
                    }
                };
                let mut state = runtime.state.lock().await;
                let outcome = persisted_tool_outcome(call, &result, result_status(&result));
                state.record.tool_outcomes.insert(call.id.clone(), outcome);
                state
                    .record
                    .messages
                    .push(ChatMessage::tool(call.id.clone(), result.text));
                state.record.touch();
                drop(state);
                self.base.store.save(runtime).await?;
            }
        }
        let request_seq = runtime.state.lock().await.prompt_seq;
        Ok((StopReason::MaxTurnRequests, usage, request_seq))
    }

    async fn command(&self, runtime: &SessionRuntime, prompt: &str) -> Result<String, Error> {
        let trimmed = prompt.trim();
        let (command, argument) = trimmed
            .split_once(char::is_whitespace)
            .map_or((trimmed, ""), |(command, argument)| {
                (command, argument.trim())
            });
        let response = match command {
            "/help" => "Available commands:\n/plan - enter read-only planning mode\n/agent - enter agent mode\n/model [name] - show or select the model\n/status - show session configuration".to_string(),
            "/plan" => {
                let mut state = runtime.state.lock().await;
                state.record.config.mode = "plan".to_string();
                state.record.touch();
                drop(state);
                self.base.store.save(runtime).await?;
                "Plan mode enabled. File mutations and commands are disabled.".to_string()
            }
            "/agent" => {
                let mut state = runtime.state.lock().await;
                state.record.config.mode = "agent".to_string();
                state.record.touch();
                drop(state);
                self.base.store.save(runtime).await?;
                "Agent mode enabled.".to_string()
            }
            "/model" if argument.is_empty() => {
                format!("Current model: {}", runtime.snapshot().await.config.model)
            }
            "/model" if self.base.config.models.iter().any(|model| model == argument) => {
                let mut state = runtime.state.lock().await;
                state.record.config.model = argument.to_string();
                state.record.touch();
                drop(state);
                self.base.store.save(runtime).await?;
                format!("Model set to {argument}.")
            }
            "/status" => {
                let record = runtime.snapshot().await;
                format!(
                    "model: {}\nmode: {}\npermission: {}\nnetwork: {}\nusage: {} tokens",
                    record.config.model,
                    record.config.mode,
                    record.config.permission_mode.as_str(),
                    record.config.network,
                    record.usage.total_tokens
                )
            }
            _ => "Unknown command. Use /help.".to_string(),
        };
        Ok(response)
    }

    async fn persist_completion(
        &self,
        runtime: &SessionRuntime,
        completion: &Completion,
        usage: TokenUsage,
    ) -> Result<(), Error> {
        {
            let mut state = runtime.state.lock().await;
            let content = (!completion.content.is_empty()).then(|| completion.content.clone());
            let reasoning = (!completion.reasoning_content.is_empty())
                .then(|| completion.reasoning_content.clone());
            let mut message = ChatMessage::assistant(content, reasoning);
            message.tool_calls = completion.tool_calls.clone();
            state.record.messages.push(message);
            state.record.usage = usage;
            if completion.usage.total_tokens > 0 {
                state.record.context_used = completion.usage.total_tokens;
            }
            state.record.touch();
        }
        self.base.store.save(runtime).await
    }

    async fn append_unfinished_tools(
        &self,
        runtime: &SessionRuntime,
        session_id: &v2::SessionId,
        connection: &V2ConnectionTo<Client>,
        calls: &[crate::model::ToolCall],
        start: usize,
        outcome_status: ToolOutcomeStatus,
    ) -> Result<(), Error> {
        for call in calls.iter().skip(start) {
            let status = match outcome_status {
                ToolOutcomeStatus::Completed => ToolCallStatus::Completed,
                ToolOutcomeStatus::Failed => ToolCallStatus::Failed,
                ToolOutcomeStatus::Cancelled => ToolCallStatus::Cancelled,
            };
            send_update(
                connection,
                session_id,
                SessionUpdate::ToolCallUpdate(
                    ToolCallUpdate::new(call.id.clone())
                        .name(call.function.name.clone())
                        .status(status),
                ),
            )?;
        }
        {
            let mut state = runtime.state.lock().await;
            for call in calls.iter().skip(start) {
                let text = match outcome_status {
                    ToolOutcomeStatus::Completed => "Tool call completed.",
                    ToolOutcomeStatus::Failed => "Tool call failed before completion.",
                    ToolOutcomeStatus::Cancelled => "Tool call was cancelled before execution.",
                }
                .to_string();
                let raw = json!({
                    "status": match outcome_status {
                        ToolOutcomeStatus::Completed => "completed",
                        ToolOutcomeStatus::Failed => "failed",
                        ToolOutcomeStatus::Cancelled => "cancelled",
                    }
                });
                let result = ToolResult {
                    text: text.clone(),
                    raw,
                };
                state.record.tool_outcomes.insert(
                    call.id.clone(),
                    persisted_tool_outcome(call, &result, outcome_status),
                );
                state
                    .record
                    .messages
                    .push(ChatMessage::tool(call.id.clone(), text));
            }
            state.record.touch();
        }
        self.base.store.save(runtime).await
    }

    async fn execute_tool(
        &self,
        runtime: &SessionRuntime,
        session_id: &v2::SessionId,
        call: &crate::model::ToolCall,
        capabilities: &v2::ClientCapabilities,
        connection: &V2ConnectionTo<Client>,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, Error> {
        let arguments: Value = match serde_json::from_str(&call.function.arguments) {
            Ok(arguments) => arguments,
            Err(error) => {
                let result = ToolResult {
                    text: format!(
                        "Tool {} received invalid JSON arguments: {error}",
                        call.function.name
                    ),
                    raw: json!({"error": "invalid arguments"}),
                };
                send_tool_result(connection, session_id, call, &result, false)?;
                return Ok(result);
            }
        };
        let record = runtime.snapshot().await;
        let title = tool_title(&call.function.name, &arguments);
        let refused_in_plan_mode =
            record.config.mode == "plan" && crate::tools::refused_in_plan_mode(&call.function.name);
        // Show what a write or edit will change before the user approves it.
        // Plan mode refuses the call below, so there is nothing to show.
        let preview = if refused_in_plan_mode {
            None
        } else {
            crate::tools::preview_diff(&record, &call.function.name, &arguments).await
        };
        let update = ToolCallUpdate::new(call.id.clone())
            .name(call.function.name.clone())
            .title(title.clone())
            .kind(tool_kind(&call.function.name))
            .status(ToolCallStatus::Pending)
            .content(preview.as_ref().map(|diff| vec![diff_content(diff)]))
            .raw_input(arguments.clone());
        send_update(
            connection,
            session_id,
            SessionUpdate::ToolCallUpdate(update.clone()),
        )?;
        if call.function.name == "update_plan" {
            let entries = parse_plan(&arguments)?;
            send_update(
                connection,
                session_id,
                SessionUpdate::PlanUpdate(PlanUpdate::new(PlanUpdateContent::items(
                    "default", entries,
                ))),
            )?;
            let result = ToolResult {
                text: "Plan updated.".to_string(),
                raw: json!({"updated": true}),
            };
            send_tool_result(connection, session_id, call, &result, true)?;
            return Ok(result);
        }
        if call.function.name == "ask_user"
            && capabilities
                .elicitation
                .as_ref()
                .is_some_and(|capabilities| {
                    capabilities.supports_form() || capabilities.supports_url()
                })
        {
            {
                let state = runtime.state.lock().await;
                state.requires_action.store(true, Ordering::Release);
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::StateUpdate(StateUpdate::RequiresAction(
                        RequiresActionStateUpdate::new(),
                    )),
                )?;
            }
            let result = self
                .ask_user(
                    connection,
                    session_id,
                    call,
                    &arguments,
                    capabilities,
                    cancellation,
                )
                .await;
            {
                let state = runtime.state.lock().await;
                state.requires_action.store(false, Ordering::Release);
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::StateUpdate(StateUpdate::Running(RunningStateUpdate::new())),
                )?;
            }
            let result = result?;
            send_tool_result(connection, session_id, call, &result, true)?;
            return Ok(result);
        }
        // Refuse before asking permission: approving a call plan mode then
        // refuses would only mislead the user.
        if refused_in_plan_mode {
            let result = ToolResult {
                text: crate::tools::PLAN_MODE_REFUSAL.to_string(),
                raw: json!({"denied": true, "reason": "plan_mode"}),
            };
            send_tool_result(connection, session_id, call, &result, false)?;
            return Ok(result);
        }
        let needs_permission = crate::tools::permission_required(&call.function.name)
            || call.function.name.starts_with("mcp__");
        let approval_key =
            crate::tools::session_approval_key(&record, &call.function.name, &arguments);
        let approved = runtime.state.lock().await.session_approvals.clone();
        if needs_permission && !crate::tools::is_auto_approved(&record, &approved, &approval_key) {
            if record.config.permission_mode == crate::config::PermissionMode::Deny {
                let result = ToolResult {
                    text: "Permission denied by policy.".to_string(),
                    raw: json!({"denied": true}),
                };
                send_tool_result(connection, session_id, call, &result, false)?;
                return Ok(result);
            }
            {
                let state = runtime.state.lock().await;
                state.requires_action.store(true, Ordering::Release);
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::StateUpdate(StateUpdate::RequiresAction(
                        RequiresActionStateUpdate::new(),
                    )),
                )?;
            }
            let decision = request_permission(
                connection,
                session_id,
                update,
                crate::tools::session_approval_label(&call.function.name),
                cancellation,
            )
            .await;
            runtime
                .state
                .lock()
                .await
                .requires_action
                .store(false, Ordering::Release);
            match decision? {
                PermissionDecision::AllowOnce => {}
                PermissionDecision::AllowAlways => {
                    runtime
                        .state
                        .lock()
                        .await
                        .session_approvals
                        .insert(approval_key);
                }
                PermissionDecision::Deny => {
                    {
                        let state = runtime.state.lock().await;
                        send_update(
                            connection,
                            session_id,
                            SessionUpdate::StateUpdate(StateUpdate::Running(
                                RunningStateUpdate::new(),
                            )),
                        )?;
                        drop(state);
                    }
                    let result = ToolResult {
                        text: "The user denied permission.".to_string(),
                        raw: json!({"denied": true}),
                    };
                    send_tool_result(connection, session_id, call, &result, false)?;
                    return Ok(result);
                }
                PermissionDecision::Cancelled => return Err(Error::request_cancelled()),
            }
            {
                let state = runtime.state.lock().await;
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::StateUpdate(StateUpdate::Running(RunningStateUpdate::new())),
                )?;
                drop(state);
            }
        }
        send_update(
            connection,
            session_id,
            SessionUpdate::ToolCallUpdate(
                ToolCallUpdate::new(call.id.clone()).status(ToolCallStatus::InProgress),
            ),
        )?;
        let access = ToolAccess {
            connection: None,
            client: Default::default(),
            config: self.base.config.clone(),
        };
        let mut result = if call.function.name.starts_with("mcp__") {
            let mcp_result = {
                let state = runtime.state.lock().await;
                let Some(mcp) = state.mcp.as_ref() else {
                    return Err(Error::internal_error().data("MCP session unavailable"));
                };
                mcp.call(&call.function.name, arguments, cancellation).await
            };
            match mcp_result {
                Ok(value) => ToolResult {
                    text: crate::mcp::mcp_result_text(&value),
                    raw: value,
                },
                Err(error) => ToolResult {
                    text: format!("MCP tool failed: {}", error.message),
                    raw: json!({"error": error.message}),
                },
            }
        } else {
            match crate::tools::execute(
                &access,
                &record,
                &call.function.name,
                arguments,
                cancellation,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => ToolResult {
                    text: format!("Tool failed: {}", error.message),
                    raw: json!({"error": error.message}),
                },
            }
        };
        if call.function.name == "run_command" {
            let terminal_id = format!("term-{}", call.id);
            send_command_terminal(connection, session_id, &terminal_id, &record, &result.raw)?;
            if let Some(raw) = result.raw.as_object_mut() {
                raw.insert("_terminal_id".to_string(), Value::String(terminal_id));
            }
        }
        let success = result.raw.get("error").is_none()
            && result
                .raw
                .get("isError")
                .and_then(Value::as_bool)
                .is_none_or(|is_error| !is_error);
        send_tool_result(connection, session_id, call, &result, success)?;
        Ok(result)
    }

    async fn ask_user(
        &self,
        connection: &V2ConnectionTo<Client>,
        session_id: &v2::SessionId,
        call: &crate::model::ToolCall,
        arguments: &Value,
        capabilities: &v2::ClientCapabilities,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, Error> {
        let message = arguments
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid_params().data("ask_user requires message"))?;
        let scope = v2::ElicitationSessionScope::new(session_id.clone())
            .tool_call_id(Some(v2::ToolCallId::new(call.id.clone())));
        let url_mode = arguments.get("url").and_then(Value::as_str);
        let mode =
            if let Some(url) = url_mode {
                if !capabilities
                    .elicitation
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.supports_url())
                {
                    return Err(Error::invalid_request()
                        .data("the Client does not support URL elicitation"));
                }
                let parsed = reqwest::Url::parse(url)
                    .map_err(|error| Error::invalid_params().data(error.to_string()))?;
                if parsed.username() != "" || parsed.password().is_some() {
                    return Err(Error::invalid_params()
                        .data("URL elicitations must not contain embedded credentials"));
                }
                if parsed.scheme() != "https" && parsed.host_str() != Some("localhost") {
                    return Err(Error::invalid_params()
                        .data("URL elicitations require HTTPS outside localhost"));
                }
                let elicitation_id = arguments
                    .get("elicitation_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| format!("elicit-{}", uuid::Uuid::new_v4().simple()));
                v2::ElicitationMode::Url(v2::ElicitationUrlMode::new(scope, elicitation_id, url))
            } else {
                if !capabilities
                    .elicitation
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.supports_form())
                {
                    return Err(Error::invalid_request()
                        .data("the Client does not support form elicitation"));
                }
                let schema = v2::ElicitationSchema::new()
                    .title("User input")
                    .description("Provide the requested information.")
                    .string("response", true);
                v2::ElicitationMode::Form(v2::ElicitationFormMode::new(scope, schema))
            };
        let request = v2::CreateElicitationRequest::new(mode, message);
        let response: v2::CreateElicitationResponse = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
            response = connection.send_request(request).block_task() => response?,
        };
        Ok(match response.action {
            v2::ElicitationAction::Accept(action) => ToolResult {
                text: action
                    .content
                    .as_ref()
                    .and_then(|content| content.get("response"))
                    .map(elicitation_text)
                    .unwrap_or_else(|| {
                        if url_mode.is_some() {
                            "The user consented to open the URL. External completion was not verified."
                                .to_string()
                        } else {
                            "The user accepted without text.".to_string()
                        }
                    }),
                raw: json!({"action": "accept"}),
            },
            v2::ElicitationAction::Decline => ToolResult {
                text: "The user declined.".to_string(),
                raw: json!({"action": "decline"}),
            },
            v2::ElicitationAction::Cancel => ToolResult {
                text: "The user cancelled.".to_string(),
                raw: json!({"action": "cancel"}),
            },
            _ => ToolResult {
                text: "Unsupported elicitation response.".to_string(),
                raw: json!({"action": "unknown"}),
            },
        })
    }

    fn config_options(&self, record: &SessionRecord) -> Vec<v2::SessionConfigOption> {
        let model_options = self
            .base
            .config
            .models
            .iter()
            .map(|model| v2::SessionConfigSelectOption::new(model.clone(), model.clone()))
            .collect::<Vec<_>>();
        vec![
            v2::SessionConfigOption::select(
                "model",
                "Model",
                record.config.model.clone(),
                model_options,
            )
            .category(v2::SessionConfigOptionCategory::Model),
            v2::SessionConfigOption::select(
                "mode",
                "Mode",
                record.config.mode.clone(),
                vec![
                    v2::SessionConfigSelectOption::new("agent", "Agent"),
                    v2::SessionConfigSelectOption::new("plan", "Plan"),
                ],
            )
            .category(v2::SessionConfigOptionCategory::Mode),
            v2::SessionConfigOption::select(
                "permission_mode",
                "Tool permissions",
                record.config.permission_mode.as_str(),
                vec![
                    v2::SessionConfigSelectOption::new("ask", "Ask"),
                    v2::SessionConfigSelectOption::new("allow", "Allow"),
                    v2::SessionConfigSelectOption::new("deny", "Deny"),
                ],
            ),
            v2::SessionConfigOption::boolean("network", "Network access", record.config.network),
        ]
    }

    async fn attach_mcp(
        &self,
        runtime: &SessionRuntime,
        servers: &[v2::McpServer],
    ) -> Result<(), Error> {
        if servers.is_empty() {
            return Ok(());
        }
        let converted = servers
            .iter()
            .map(convert_mcp_server)
            .collect::<Result<Vec<_>, _>>()?;
        self.base.attach_mcp(runtime, &converted).await
    }
}

fn send_update(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    update: SessionUpdate,
) -> Result<(), Error> {
    connection.send_notification(UpdateSessionNotification::new(session_id.clone(), update))
}

fn send_chunk(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    message_id: &str,
    text: String,
    thought: bool,
) -> Result<(), Error> {
    if text.is_empty() {
        return Ok(());
    }
    let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)), message_id);
    send_update(
        connection,
        session_id,
        if thought {
            SessionUpdate::AgentThoughtChunk(chunk)
        } else {
            SessionUpdate::AgentMessageChunk(chunk)
        },
    )
}

fn send_agent_text(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    text: &str,
) -> Result<(), Error> {
    send_chunk(
        connection,
        session_id,
        &format!("message-{}", uuid::Uuid::new_v4().simple()),
        text.to_string(),
        false,
    )
}

fn send_command_terminal(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    terminal_id: &str,
    record: &SessionRecord,
    raw: &Value,
) -> Result<(), Error> {
    let command = crate::tools::command_display(raw);
    let cwd = raw
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| record.cwd.clone());
    let stdout = raw
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stderr = raw
        .get("stderr")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut output = String::with_capacity(stdout.len() + stderr.len());
    output.push_str(stdout);
    if !stderr.is_empty() && !stdout.is_empty() && !stdout.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(stderr);
    let failed = raw.get("error").is_some();
    let exit_status = (!failed && (raw.get("exit_code").is_some() || raw.get("signal").is_some()))
        .then(|| {
            TerminalExitStatus::new()
                .exit_code(
                    raw.get("exit_code")
                        .and_then(Value::as_u64)
                        .and_then(|code| u32::try_from(code).ok()),
                )
                .signal(
                    raw.get("signal")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                )
        });
    send_update(
        connection,
        session_id,
        SessionUpdate::TerminalUpdate(
            TerminalUpdate::new(terminal_id)
                .command(Some(command.clone()))
                .cwd(Some(v2::AbsolutePath::new(cwd))),
        ),
    )?;
    for chunk in output.as_bytes().chunks(8192) {
        send_update(
            connection,
            session_id,
            SessionUpdate::TerminalOutputChunk(TerminalOutputChunk::new(
                terminal_id,
                base64::engine::general_purpose::STANDARD.encode(chunk),
            )),
        )?;
    }
    if let Some(exit_status) = exit_status {
        send_update(
            connection,
            session_id,
            SessionUpdate::TerminalUpdate(
                TerminalUpdate::new(terminal_id)
                    .command(Some(command))
                    .exit_status(Some(exit_status)),
            ),
        )?;
    }
    Ok(())
}

fn send_persisted_terminal(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    record: &SessionRecord,
    terminal: &crate::model::PersistedTerminal,
) -> Result<(), Error> {
    let cwd = if terminal.cwd.as_os_str().is_empty() {
        record.cwd.clone()
    } else {
        terminal.cwd.clone()
    };
    let exit_status = terminal.exited.then(|| {
        TerminalExitStatus::new()
            .exit_code(terminal.exit_code)
            .signal(terminal.signal.clone())
    });
    send_update(
        connection,
        session_id,
        SessionUpdate::TerminalUpdate(
            TerminalUpdate::new(terminal.terminal_id.clone())
                .command(Some(terminal.command.clone()))
                .cwd(Some(v2::AbsolutePath::new(cwd)))
                .output(Some(TerminalOutput::new(
                    base64::engine::general_purpose::STANDARD.encode(terminal.output.as_bytes()),
                )))
                .exit_status(exit_status),
        ),
    )
}

fn send_tool_result(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    call: &crate::model::ToolCall,
    result: &ToolResult,
    success: bool,
) -> Result<(), Error> {
    if result.text.chars().count() > 4096 {
        let mut chunk = String::new();
        let mut chunk_chars = 0usize;
        for character in result.text.chars() {
            chunk.push(character);
            chunk_chars += 1;
            if chunk_chars == 4096 {
                send_update(
                    connection,
                    session_id,
                    SessionUpdate::ToolCallContentChunk(ToolCallContentChunk::new(
                        call.id.clone(),
                        ToolCallContent::Content(Box::new(v2::Content::new(ContentBlock::Text(
                            TextContent::new(std::mem::take(&mut chunk)),
                        )))),
                    )),
                )?;
                chunk_chars = 0;
            }
        }
        if !chunk.is_empty() {
            send_update(
                connection,
                session_id,
                SessionUpdate::ToolCallContentChunk(ToolCallContentChunk::new(
                    call.id.clone(),
                    ToolCallContent::Content(Box::new(v2::Content::new(ContentBlock::Text(
                        TextContent::new(chunk),
                    )))),
                )),
            )?;
        }
    }
    let mut content = vec![ToolCallContent::Content(Box::new(v2::Content::new(
        ContentBlock::Text(TextContent::new(result.text.clone())),
    )))];
    for item in structured_content(&result.raw) {
        if let Ok(block) = serde_json::from_value::<ContentBlock>(item) {
            content.push(ToolCallContent::Content(Box::new(v2::Content::new(block))));
        }
    }
    if let Some(terminal_id) = result.raw.get("_terminal_id").and_then(Value::as_str) {
        content.push(ToolCallContent::Terminal(Terminal::new(terminal_id)));
    }
    if let Some(diff) = persisted_diff(&call.function.name, &result.raw) {
        content.push(diff_content(&diff));
    }
    send_update(
        connection,
        session_id,
        SessionUpdate::ToolCallUpdate(
            ToolCallUpdate::new(call.id.clone())
                .status(if success {
                    ToolCallStatus::Completed
                } else {
                    ToolCallStatus::Failed
                })
                .content(Some(content))
                .raw_output(Some(result.raw.clone())),
        ),
    )
}

/// A file change as ACP v2 tool call content: a git-style patch of the whole
/// file.
fn diff_content(diff: &PersistedDiff) -> ToolCallContent {
    let operation = if diff.old_text.is_some() {
        DiffChange::modify(&diff.path)
    } else {
        DiffChange::add(&diff.path)
    }
    .file_type(Some(DiffFileType::Text));
    ToolCallContent::Diff(Diff::patch(git_patch(diff), vec![operation]))
}

enum PermissionDecision {
    AllowOnce,
    AllowAlways,
    Deny,
    Cancelled,
}

async fn request_permission(
    connection: &V2ConnectionTo<Client>,
    session_id: &v2::SessionId,
    tool_call: ToolCallUpdate,
    session_approval_label: &str,
    cancellation: &CancellationToken,
) -> Result<PermissionDecision, Error> {
    let options = vec![
        PermissionOption::new(
            v2::PermissionOptionId::new("allow_once"),
            "Allow once",
            PermissionOptionKind::AllowOnce,
        ),
        PermissionOption::new(
            v2::PermissionOptionId::new("allow_always"),
            session_approval_label,
            PermissionOptionKind::AllowAlways,
        ),
        PermissionOption::new(
            v2::PermissionOptionId::new("reject"),
            "Deny",
            PermissionOptionKind::RejectOnce,
        ),
    ];
    let request = RequestPermissionRequest::new(session_id.clone(), "Run sensitive tool", options)
        .subject(Some(v2::RequestPermissionSubject::from(tool_call)));
    let response: v2::RequestPermissionResponse = tokio::select! {
        _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
        response = connection.send_request(request).block_task() => response?,
    };
    Ok(match response.outcome {
        RequestPermissionOutcome::Cancelled => PermissionDecision::Cancelled,
        RequestPermissionOutcome::Selected(v2::SelectedPermissionOutcome { option_id, .. }) => {
            match option_id.to_string().as_str() {
                "allow_once" => PermissionDecision::AllowOnce,
                "allow_always" => PermissionDecision::AllowAlways,
                _ => PermissionDecision::Deny,
            }
        }
        _ => PermissionDecision::Deny,
    })
}

fn replay_history(
    connection: &V2ConnectionTo<Client>,
    record: &SessionRecord,
) -> Result<(), Error> {
    let mut pending_calls = Vec::<String>::new();
    for (index, message) in record.messages.iter().enumerate() {
        let id = record
            .message_ids
            .get(&index.to_string())
            .cloned()
            .unwrap_or_else(|| message_id_for(&record.id, index));
        match message.role {
            Role::User => {
                if let Some(content) = &message.content {
                    let blocks = record
                        .prompt_blocks
                        .get(&index.to_string())
                        .and_then(|blocks| {
                            serde_json::from_value::<Vec<ContentBlock>>(Value::Array(
                                blocks.clone(),
                            ))
                            .ok()
                        })
                        .unwrap_or_else(|| message_content_to_v2_blocks(content));
                    send_update(
                        connection,
                        &v2_session_id(&record.id),
                        SessionUpdate::UserMessage(v2::UserMessage::new(id).content(blocks)),
                    )?;
                }
            }
            Role::Assistant => {
                if let Some(reasoning) = &message.reasoning_content {
                    send_update(
                        connection,
                        &v2_session_id(&record.id),
                        SessionUpdate::AgentThought(
                            AgentThought::new(format!("{id}-thought"))
                                .content(vec![ContentBlock::Text(TextContent::new(reasoning))]),
                        ),
                    )?;
                }
                if let Some(content) = &message.content {
                    send_update(
                        connection,
                        &v2_session_id(&record.id),
                        SessionUpdate::AgentMessage(AgentMessage::new(id.clone()).content(vec![
                            ContentBlock::Text(TextContent::new(content.text_value())),
                        ])),
                    )?;
                }
                for call in &message.tool_calls {
                    let input = serde_json::from_str(&call.function.arguments)
                        .unwrap_or_else(|_| Value::String(call.function.arguments.clone()));
                    send_update(
                        connection,
                        &v2_session_id(&record.id),
                        SessionUpdate::ToolCallUpdate(
                            ToolCallUpdate::new(call.id.clone())
                                .name(call.function.name.clone())
                                .title(call.function.name.clone())
                                .status(ToolCallStatus::Pending)
                                .raw_input(input),
                        ),
                    )?;
                    if call.function.name == "update_plan"
                        && let Ok(arguments) =
                            serde_json::from_str::<Value>(&call.function.arguments)
                        && let Ok(entries) = parse_plan(&arguments)
                    {
                        send_update(
                            connection,
                            &v2_session_id(&record.id),
                            SessionUpdate::PlanUpdate(PlanUpdate::new(PlanUpdateContent::items(
                                "default", entries,
                            ))),
                        )?;
                    }
                    pending_calls.push(call.id.clone());
                }
            }
            Role::Tool => {
                if let Some(tool_call_id) = &message.tool_call_id {
                    let outcome = record.tool_outcomes.get(tool_call_id);
                    let status =
                        outcome.map_or(ToolCallStatus::Failed, |outcome| match outcome.status {
                            ToolOutcomeStatus::Completed => ToolCallStatus::Completed,
                            ToolOutcomeStatus::Failed => ToolCallStatus::Failed,
                            ToolOutcomeStatus::Cancelled => ToolCallStatus::Cancelled,
                        });
                    let mut content = vec![ToolCallContent::Content(Box::new(v2::Content::new(
                        ContentBlock::Text(TextContent::new(
                            message
                                .content
                                .as_ref()
                                .map(crate::model::MessageContent::text_value)
                                .unwrap_or_default(),
                        )),
                    )))];
                    if let Some(outcome) = outcome {
                        for item in &outcome.structured_content {
                            if let Ok(block) = serde_json::from_value::<ContentBlock>(item.clone())
                            {
                                content.push(ToolCallContent::Content(Box::new(v2::Content::new(
                                    block,
                                ))));
                            }
                        }
                        if let Some(diff) = &outcome.diff {
                            content.push(diff_content(diff));
                        }
                        if let Some(terminal) = &outcome.terminal {
                            content.push(ToolCallContent::Terminal(Terminal::new(
                                terminal.terminal_id.clone(),
                            )));
                            send_persisted_terminal(
                                connection,
                                &v2_session_id(&record.id),
                                record,
                                terminal,
                            )?;
                        }
                    }
                    send_update(
                        connection,
                        &v2_session_id(&record.id),
                        SessionUpdate::ToolCallUpdate(
                            ToolCallUpdate::new(tool_call_id.clone())
                                .status(status)
                                .content(Some(content))
                                .raw_output(outcome.map(|outcome| outcome.raw_output.clone())),
                        ),
                    )?;
                    pending_calls.retain(|id| id != tool_call_id);
                }
            }
            Role::System => {}
        }
    }
    for tool_call_id in pending_calls {
        send_update(
            connection,
            &v2_session_id(&record.id),
            SessionUpdate::ToolCallUpdate(
                ToolCallUpdate::new(tool_call_id).status(ToolCallStatus::Failed),
            ),
        )?;
    }
    for pending in &record.pending_steering {
        let blocks =
            serde_json::from_value::<Vec<ContentBlock>>(Value::Array(pending.blocks.clone()))
                .unwrap_or_else(|_| message_content_to_v2_blocks(&pending.content));
        send_update(
            connection,
            &v2_session_id(&record.id),
            SessionUpdate::UserMessage(
                v2::UserMessage::new(pending.message_id.clone()).content(blocks),
            ),
        )?;
    }
    send_update(
        connection,
        &v2_session_id(&record.id),
        SessionUpdate::SessionInfoUpdate(v2::SessionInfoUpdate::new().title(record.title.clone())),
    )?;
    for compaction in &record.compactions {
        let status = match compaction.status.as_str() {
            "failed" => CompactionStatus::Failed,
            "cancelled" => CompactionStatus::Cancelled,
            "in_progress" => CompactionStatus::InProgress,
            _ => CompactionStatus::Completed,
        };
        send_update(
            connection,
            &v2_session_id(&record.id),
            SessionUpdate::CompactionUpdate(CompactionUpdate::new(compaction.id.clone(), status)),
        )?;
    }
    send_update(
        connection,
        &v2_session_id(&record.id),
        SessionUpdate::UsageUpdate(v2::UsageUpdate::new(record.context_used, CONTEXT_WINDOW)),
    )
}

fn message_id_for(session_id: &str, index: usize) -> String {
    format!("{session_id}-m{index}")
}

fn v2_session_id(session_id: &str) -> v2::SessionId {
    v2::SessionId::new(session_id.to_string())
}

fn message_content_to_v2_blocks(content: &MessageContent) -> Vec<ContentBlock> {
    match content {
        MessageContent::Text(text) => {
            vec![ContentBlock::Text(TextContent::new(text))]
        }
        MessageContent::Parts(parts) => parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(ContentBlock::Text(TextContent::new(text))),
                ContentPart::ImageUrl { image_url } => {
                    let data_url = image_url.url.strip_prefix("data:")?;
                    let (mime_type, data) = data_url.split_once(";base64,")?;
                    Some(ContentBlock::Image(v2::ImageContent::new(data, mime_type)))
                }
                ContentPart::File { file_data, .. } => {
                    let data_url = file_data.as_deref()?.strip_prefix("data:")?;
                    let (mime_type, data) = data_url.split_once(";base64,")?;
                    Some(ContentBlock::Image(v2::ImageContent::new(data, mime_type)))
                }
            })
            .collect(),
    }
}

fn available_commands() -> Vec<AvailableCommand> {
    vec![
        AvailableCommand::new("help", "Show available harness commands"),
        AvailableCommand::new("plan", "Enter read-only planning mode"),
        AvailableCommand::new("agent", "Enter agent mode"),
        AvailableCommand::new("model", "Show or select the model"),
        AvailableCommand::new("status", "Show session configuration"),
    ]
}

fn v2_session_info(record: SessionRecord) -> v2::SessionInfo {
    let title = session_title(&record);
    v2::SessionInfo::new(record.id, record.cwd)
        .additional_directories(record.additional_directories)
        .title(title)
        .updated_at(record.updated_at.to_rfc3339())
}

fn absolute_paths(paths: Vec<v2::AbsolutePath>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .map(v2::AbsolutePath::into_inner)
        .collect()
}

fn v2_system_prompt_text(prompt: Option<&v2::SystemPrompt>) -> Result<Option<String>, Error> {
    let Some(prompt) = prompt else {
        return Ok(None);
    };
    if !matches!(prompt.mode, v2::SystemPromptMode::Append) {
        return Err(
            Error::invalid_params().data("only append mode is supported for client system prompts")
        );
    }
    let text = prompt
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Text(text) => Ok(text.text.as_str()),
            _ => Err(Error::invalid_params().data("client system prompts may contain only text")),
        })
        .collect::<Result<Vec<_>, _>>()?
        .join("\n\n");
    Ok(Some(text))
}

struct V2PromptPayload {
    text: String,
    content: MessageContent,
    has_images: bool,
}

fn content_blocks_to_prompt(blocks: &[ContentBlock]) -> Result<V2PromptPayload, Error> {
    let mut text_parts = Vec::new();
    let mut content_parts = Vec::new();
    let mut has_images = false;
    for block in blocks {
        match block {
            ContentBlock::Text(text) => {
                text_parts.push(text.text.clone());
                content_parts.push(ContentPart::Text {
                    text: text.text.clone(),
                });
            }
            ContentBlock::ResourceLink(link) => {
                let text = format!("[resource: {} ({})]", link.name, link.uri);
                text_parts.push(text.clone());
                content_parts.push(ContentPart::Text { text });
            }
            ContentBlock::Resource(resource) => match &resource.resource {
                v2::EmbeddedResourceResource::TextResourceContents(text) => {
                    let text = format!("[embedded resource: {}]\n{}", text.uri, text.text);
                    text_parts.push(text.clone());
                    content_parts.push(ContentPart::Text { text });
                }
                v2::EmbeddedResourceResource::BlobResourceContents(blob) => {
                    let mime = blob
                        .mime_type
                        .as_ref()
                        .map(|mime| mime.as_ref())
                        .unwrap_or("application/octet-stream");
                    if mime.starts_with("image/") {
                        has_images = true;
                        content_parts.push(ContentPart::ImageUrl {
                            image_url: ImageUrl {
                                url: format!("data:{mime};base64,{}", blob.blob),
                                detail: None,
                            },
                        });
                    } else {
                        let text = format!(
                            "[embedded binary resource: {}, mime={}, {} base64 bytes]",
                            blob.uri,
                            mime,
                            blob.blob.len()
                        );
                        text_parts.push(text.clone());
                        content_parts.push(ContentPart::Text { text });
                    }
                }
                _ => {
                    let text = "[unsupported resource]".to_string();
                    text_parts.push(text.clone());
                    content_parts.push(ContentPart::Text { text });
                }
            },
            ContentBlock::Image(image) => {
                has_images = true;
                content_parts.push(ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: format!("data:{};base64,{}", image.mime_type, image.data),
                        detail: None,
                    },
                });
            }
            ContentBlock::Audio(audio) => {
                let text = format!(
                    "[audio omitted: mime={}, {} base64 bytes]",
                    audio.mime_type,
                    audio.data.len()
                );
                text_parts.push(text.clone());
                content_parts.push(ContentPart::Text { text });
            }
            _ => {
                let text = "[unsupported content]".to_string();
                text_parts.push(text.clone());
                content_parts.push(ContentPart::Text { text });
            }
        }
    }
    let text = text_parts.join("\n\n");
    let content = if has_images {
        MessageContent::Parts(content_parts)
    } else {
        MessageContent::Text(text.clone())
    };
    Ok(V2PromptPayload {
        text,
        content,
        has_images,
    })
}

fn apply_config(
    record: &mut SessionRecord,
    config_id: &str,
    value: &v2::SessionConfigOptionValue,
    models: &[String],
) -> Result<(), Error> {
    match (config_id, value) {
        ("model", v2::SessionConfigOptionValue::Id { value })
            if models.iter().any(|model| model == value.0.as_ref()) =>
        {
            record.config.model = value.to_string();
            Ok(())
        }
        ("mode", v2::SessionConfigOptionValue::Id { value })
            if matches!(value.to_string().as_str(), "agent" | "plan") =>
        {
            record.config.mode = value.to_string();
            Ok(())
        }
        ("permission_mode", v2::SessionConfigOptionValue::Id { value }) => {
            record.config.permission_mode = value
                .to_string()
                .parse()
                .map_err(|()| Error::invalid_params().data("invalid permission policy"))?;
            Ok(())
        }
        ("network", v2::SessionConfigOptionValue::Boolean { value }) => {
            record.config.network = *value;
            Ok(())
        }
        _ => Err(Error::invalid_params().data("unknown or invalid config option")),
    }
}

fn parse_plan(arguments: &Value) -> Result<Vec<PlanEntry>, Error> {
    let values = arguments
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid_params().data("update_plan requires entries"))?;
    values
        .iter()
        .map(|entry| {
            let content = entry
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::invalid_params().data("plan entry requires content"))?;
            let priority = match entry.get("priority").and_then(Value::as_str) {
                Some("high") => PlanEntryPriority::High,
                Some("low") => PlanEntryPriority::Low,
                _ => PlanEntryPriority::Medium,
            };
            let status = match entry.get("status").and_then(Value::as_str) {
                Some("in_progress") => PlanEntryStatus::InProgress,
                Some("completed") => PlanEntryStatus::Completed,
                Some("cancelled") => PlanEntryStatus::Cancelled,
                _ => PlanEntryStatus::Pending,
            };
            Ok(PlanEntry::new(content, priority, status))
        })
        .collect()
}

fn convert_mcp_server(
    server: &v2::McpServer,
) -> Result<agent_client_protocol::schema::v1::McpServer, Error> {
    use agent_client_protocol::schema::v1;
    match server {
        v2::McpServer::Stdio(config) => Ok(v1::McpServer::Stdio(
            v1::McpServerStdio::new(&config.name, config.command.0.clone())
                .args(config.args.clone())
                .env(
                    config
                        .env
                        .iter()
                        .map(|env| v1::EnvVariable::new(&env.name, &env.value))
                        .collect(),
                ),
        )),
        v2::McpServer::Http(config) => Ok(v1::McpServer::Http(
            v1::McpServerHttp::new(&config.name, &config.url).headers(
                config
                    .headers
                    .iter()
                    .map(|header| v1::HttpHeader::new(&header.name, &header.value))
                    .collect(),
            ),
        )),
        _ => Err(Error::invalid_params()
            .data("this v2 MCP transport is not supported by the standalone agent")),
    }
}

fn tool_kind(name: &str) -> v2::ToolKind {
    match name {
        "read_file" => v2::ToolKind::Read,
        "write_file" | "edit_file" => v2::ToolKind::Edit,
        "search_files" | "list_directory" => v2::ToolKind::Search,
        "run_command" => v2::ToolKind::Execute,
        "web_fetch" => v2::ToolKind::Fetch,
        _ => v2::ToolKind::Other,
    }
}

fn tool_title(name: &str, arguments: &Value) -> String {
    if name == "run_command" {
        return format!("run command {}", crate::tools::command_display(arguments));
    }
    arguments
        .get("path")
        .or_else(|| arguments.get("url"))
        .or_else(|| arguments.get("command"))
        .and_then(Value::as_str)
        .map_or_else(
            || name.replace('_', " "),
            |target| format!("{} {target}", name.replace('_', " ")),
        )
}

fn v2_usage(usage: TokenUsage) -> v2::Usage {
    v2::Usage::new(
        usage.total_tokens,
        usage.prompt_tokens,
        usage.completion_tokens,
    )
    .cached_read_tokens(Some(usage.prompt_cache_hit_tokens))
}

fn v2_error_from(error: Error) -> v2::Error {
    serde_json::to_value(error)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_else(v2::Error::internal_error)
}

fn elicitation_text(value: &v2::ElicitationContentValue) -> String {
    match value {
        v2::ElicitationContentValue::String(value) => value.clone(),
        v2::ElicitationContentValue::Integer(value) => value.to_string(),
        v2::ElicitationContentValue::Number(value) => value.to_string(),
        v2::ElicitationContentValue::Boolean(value) => value.to_string(),
        v2::ElicitationContentValue::StringArray(values) => values.join(", "),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "<value>".to_string()),
    }
}
