use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use agent_client_protocol::{
    Agent, ByteStreams, Client, ConnectTo, ConnectionTo, Error, JsonRpcMessage, UntypedMessage,
    schema::ProtocolVersion,
    schema::v1::{
        AgentAuthCapabilities, AgentCapabilities, AuthMethod, AuthMethodAgent, AuthenticateRequest,
        AuthenticateResponse, AvailableCommand, AvailableCommandsUpdate, CancelNotification,
        ClientCapabilities, CloseSessionRequest, CloseSessionResponse, CompactionStatus,
        CompactionUpdate, ConfigOptionUpdate, ContentBlock, ContentChunk, CreateElicitationRequest,
        CreateElicitationResponse, CurrentModeUpdate, DeleteSessionRequest, DeleteSessionResponse,
        DisableProviderRequest, DisableProviderResponse, ElicitationAction,
        ElicitationContentValue, ElicitationFormMode, ElicitationMode, ElicitationSchema,
        ElicitationSessionScope, ElicitationUrlMode, EmbeddedResourceResource, ForkSessionRequest,
        ForkSessionResponse, Implementation, InitializeRequest, InitializeResponse,
        ListProvidersRequest, ListProvidersResponse, ListSessionsRequest, ListSessionsResponse,
        LlmProtocol, LoadSessionRequest, LoadSessionResponse, LogoutCapabilities, LogoutRequest,
        LogoutResponse, McpCapabilities, NewSessionRequest, NewSessionResponse, Notice,
        NoticeSeverity, PermissionOption, PermissionOptionId, PermissionOptionKind, Plan,
        PlanEntry, PlanEntryPriority, PlanEntryStatus, PromptCapabilities, PromptRequest,
        PromptResponse, ProviderCurrentConfig, ProviderId, ProviderInfo, ProvidersCapabilities,
        RequestPermissionOutcome, RequestPermissionRequest, ResumeSessionRequest,
        ResumeSessionResponse, SelectedPermissionOutcome, SessionAdditionalDirectoriesCapabilities,
        SessionCapabilities, SessionCloseCapabilities, SessionConfigOption,
        SessionConfigOptionCategory, SessionConfigOptionValue, SessionConfigSelectOption,
        SessionDeleteCapabilities, SessionForkCapabilities, SessionId, SessionInfo,
        SessionInfoUpdate, SessionListCapabilities, SessionMode, SessionModeId, SessionModeState,
        SessionNotification, SessionResumeCapabilities, SessionSystemPromptCapabilities,
        SessionSystemPromptModeCapabilities, SessionSystemPromptOverrideCapabilities,
        SetProviderRequest, SetProviderResponse, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse, StopReason,
        SystemPromptMode, Terminal, TerminalId, TextContent, ToolCall, ToolCallContent, ToolCallId,
        ToolCallLocation, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind, Usage,
        UsageUpdate,
    },
};
use chrono::SecondsFormat;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    auth_status,
    config::{Config, PermissionMode, ReasoningEffort},
    deepseek::{Completion, DeepSeekClient, StreamEvent},
    mcp::{McpSession, mcp_result_text},
    model::{
        ChatMessage, ClientPromptMode, ClientSystemPrompt, ContentPart, ImageUrl, MessageContent,
        PendingUserMessage, PersistedCompaction, PersistedDiff, Role, SessionRecord,
        SessionRuntime, TokenUsage, ToolOutcomeStatus, ensure_session_unique_tool_call_ids,
    },
    session::{self, SessionPresence, SessionStore, session_title},
    session_status,
    tools::{
        TerminalToolCall, ToolAccess, ToolResult, built_in_definitions, execute as execute_tool,
        persisted_tool_outcome, result_status, structured_content,
    },
};

const PAGE_SIZE: usize = 100;
const DEFAULT_CONTEXT_WINDOW: u64 = 1_000_000;
const MAX_MODEL_TOOLS: usize = 128;
const STEERING_METHOD: &str = "_session/steering";

#[derive(Clone)]
pub(crate) struct Harness {
    pub(crate) config: Config,
    pub(crate) store: SessionStore,
    pub(crate) client_capabilities: Arc<tokio::sync::RwLock<ClientCapabilities>>,
    pub(crate) initialized: Arc<AtomicBool>,
    pub(crate) provider: Arc<tokio::sync::RwLock<ProviderRouting>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderRouting {
    pub(crate) enabled: bool,
    pub(crate) base_url: String,
    pub(crate) headers: std::collections::HashMap<String, String>,
}

/// Where the credential for DeepSeek requests comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Credential {
    /// An `Authorization` header from the provider configuration. It takes
    /// precedence over the API key.
    ProviderHeader,
    /// `DEEPSEEK_API_KEY` or `--api-key`.
    ApiKey,
}

impl ProviderRouting {
    /// The credential DeepSeek requests would carry, whether or not the
    /// provider is enabled. A blank API key does not count.
    pub(crate) fn credential(&self, config: &Config) -> Option<Credential> {
        if self
            .headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
        {
            Some(Credential::ProviderHeader)
        } else if config
            .api_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
        {
            Some(Credential::ApiKey)
        } else {
            None
        }
    }
}

pub async fn run(config: Config) -> agent_client_protocol::Result<()> {
    let filter = if config.trace {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("dsh_acp=trace,info"))
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("dsh_acp=info,warn"))
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();

    let store = SessionStore::open(&config).await?;
    let provider = ProviderRouting {
        enabled: true,
        base_url: config.base_url.clone(),
        headers: std::collections::HashMap::new(),
    };
    let harness = Harness {
        config,
        store,
        client_capabilities: Arc::new(tokio::sync::RwLock::new(ClientCapabilities::default())),
        initialized: Arc::new(AtomicBool::new(false)),
        provider: Arc::new(tokio::sync::RwLock::new(provider)),
    };

    let v1_agent = Agent
        .builder()
        .name("dsh-acp")
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: AuthenticateRequest,
                            responder: agent_client_protocol::Responder<AuthenticateResponse>,
                            _connection: ConnectionTo<Client>| {
                    harness.clone().authenticate(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: LogoutRequest,
                            responder: agent_client_protocol::Responder<LogoutResponse>,
                            _connection: ConnectionTo<Client>| {
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
                            _connection: ConnectionTo<Client>| {
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
                            _connection: ConnectionTo<Client>| {
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
                            _connection: ConnectionTo<Client>| {
                    harness.clone().disable_provider(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: NewSessionRequest,
                            responder: agent_client_protocol::Responder<NewSessionResponse>,
                            connection: ConnectionTo<Client>| {
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
                async move |request: LoadSessionRequest,
                            responder: agent_client_protocol::Responder<LoadSessionResponse>,
                            connection: ConnectionTo<Client>| {
                    harness
                        .clone()
                        .load_session(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: ResumeSessionRequest,
                            responder: agent_client_protocol::Responder<ResumeSessionResponse>,
                            connection: ConnectionTo<Client>| {
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
                async move |request: ForkSessionRequest,
                            responder: agent_client_protocol::Responder<ForkSessionResponse>,
                            connection: ConnectionTo<Client>| {
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
                async move |request: ListSessionsRequest,
                            responder: agent_client_protocol::Responder<ListSessionsResponse>,
                            _connection: ConnectionTo<Client>| {
                    harness.clone().list_sessions(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: DeleteSessionRequest,
                            responder: agent_client_protocol::Responder<DeleteSessionResponse>,
                            _connection: ConnectionTo<Client>| {
                    harness.clone().delete_session(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: CloseSessionRequest,
                            responder: agent_client_protocol::Responder<CloseSessionResponse>,
                            _connection: ConnectionTo<Client>| {
                    harness.clone().close_session(request, responder).await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: SetSessionModeRequest,
                            responder: agent_client_protocol::Responder<SetSessionModeResponse>,
                            connection: ConnectionTo<Client>| {
                    harness
                        .clone()
                        .set_session_mode(request, responder, connection)
                        .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let harness = harness.clone();
                async move |request: SetSessionConfigOptionRequest,
                            responder: agent_client_protocol::Responder<
                    SetSessionConfigOptionResponse,
                >,
                            connection: ConnectionTo<Client>| {
                    harness
                        .clone()
                        .set_session_config(request, responder, connection)
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
                            connection: ConnectionTo<Client>| {
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
                            connection: ConnectionTo<Client>| {
                    match request.method.as_str() {
                        // Answered as JSON: the SDK capability types cannot advertise
                        // `auth/status` or `session/status` yet.
                        "initialize" => {
                            match InitializeRequest::parse_message(&request.method, &request.params)
                            {
                                Ok(initialize) => {
                                    harness
                                        .clone()
                                        .initialize(initialize, responder, connection)
                                        .await
                                }
                                Err(error) => responder.respond_with_error(error),
                            }
                        }
                        STEERING_METHOD => {
                            harness
                                .clone()
                                .steering(request.params, responder, connection)
                                .await
                        }
                        session_status::METHOD => {
                            harness
                                .clone()
                                .session_status(request.params, responder)
                                .await
                        }
                        auth_status::METHOD => responder.respond(harness.auth_status().await),
                        _ => responder.respond_with_error(Error::method_not_found()),
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let harness = harness.clone();
                async move |notification: CancelNotification, _connection: ConnectionTo<Client>| {
                    harness.clone().cancel(notification).await;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        );

    Agent
        .protocol_router()
        .with_v1(v1_agent)
        .with_v2(crate::v2_agent::component(harness.clone()))
        .connect_to(stdio_transport())
        .await
}

/// Serializes an `initialize` response and adds capability markers the SDK's
/// capability types have no field for yet. Each marker is the JSON pointer of
/// an existing capabilities object, a field name, and its value.
pub(crate) fn with_extra_capabilities(
    response: impl serde::Serialize,
    markers: &[(&str, &str, Value)],
) -> Result<Value, Error> {
    let mut response = serde_json::to_value(response).map_err(Error::into_internal_error)?;
    for (pointer, name, value) in markers {
        response
            .pointer_mut(pointer)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                Error::internal_error().data(format!("initialize response has no {pointer}"))
            })?
            .insert((*name).to_string(), value.clone());
    }
    Ok(response)
}

/// Stdio as a transport the protocol router can finish.
///
/// When the router rejects `initialize`, it asks the transport to finish so
/// the error response is written before the process exits. The SDK's `Stdio`
/// cannot be asked to finish, so the router stops it after one poll, which
/// races the write: the client often got no response at all.
fn stdio_transport() -> impl ConnectTo<Agent> {
    ByteStreams::new(
        blocking::Unblock::new(std::io::stdout()),
        blocking::Unblock::new(std::io::stdin()),
    )
}

impl Harness {
    async fn initialize(
        self,
        request: InitializeRequest,
        responder: agent_client_protocol::Responder<Value>,
        _connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        if request.protocol_version == ProtocolVersion::V0 {
            return responder.respond_with_error(
                Error::invalid_params().data("ACP protocol v0 is unsupported"),
            );
        }
        *self.client_capabilities.write().await = request.client_capabilities.clone();
        self.initialized.store(true, Ordering::Release);

        let session_capabilities = SessionCapabilities::new()
            .list(SessionListCapabilities::new())
            .delete(SessionDeleteCapabilities::new())
            .additional_directories(SessionAdditionalDirectoriesCapabilities::new())
            .resume(SessionResumeCapabilities::new())
            .close(SessionCloseCapabilities::new())
            .fork(SessionForkCapabilities::new())
            .system_prompt(
                SessionSystemPromptCapabilities::new().mode(
                    SessionSystemPromptModeCapabilities::new()
                        .override_(SessionSystemPromptOverrideCapabilities::new()),
                ),
            );
        let capabilities = AgentCapabilities::new()
            .load_session(true)
            .prompt_capabilities(
                PromptCapabilities::new()
                    .image(
                        self.config
                            .models
                            .iter()
                            .any(|model| Config::model_supports_images(model)),
                    )
                    .embedded_context(true),
            )
            .mcp_capabilities(McpCapabilities::new().http(true).sse(false))
            .session_capabilities(session_capabilities)
            .auth(AgentAuthCapabilities::new().logout(LogoutCapabilities::new()))
            .providers(ProvidersCapabilities::new());

        let response = InitializeResponse::new(ProtocolVersion::V1)
            .agent_capabilities(capabilities)
            .auth_methods(vec![AuthMethod::Agent(
                AuthMethodAgent::new("deepseek-api-key", "DeepSeek API key").description(
                    "Uses DEEPSEEK_API_KEY, --api-key, or an Authorization provider header.",
                ),
            )])
            .agent_info(
                Implementation::new("dsh-acp", env!("CARGO_PKG_VERSION")).title("DeepSeek Harness"),
            )
            .meta(Some(serde_json::Map::from_iter([(
                "steering".to_string(),
                json!({"supported": true, "idleBehaviors": ["promptRequired"]}),
            )])));
        match with_extra_capabilities(
            response,
            &[
                (
                    "/agentCapabilities/sessionCapabilities",
                    "status",
                    json!({}),
                ),
                ("/agentCapabilities/auth", "status", json!(true)),
            ],
        ) {
            Ok(response) => responder.respond(response),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn authenticate(
        self,
        request: AuthenticateRequest,
        responder: agent_client_protocol::Responder<AuthenticateResponse>,
    ) -> agent_client_protocol::Result<()> {
        if request.method_id.to_string() != "deepseek-api-key" {
            return responder
                .respond_with_error(Error::invalid_params().data("unknown authentication method"));
        }
        let has_credential = self
            .provider
            .read()
            .await
            .credential(&self.config)
            .is_some();
        if has_credential {
            self.provider.write().await.enabled = true;
            responder.respond(AuthenticateResponse::new())
        } else {
            responder.respond_with_error(Error::auth_required().data(
                "Configure DEEPSEEK_API_KEY or provider Authorization headers before authenticating",
            ))
        }
    }

    async fn logout(
        self,
        _request: LogoutRequest,
        responder: agent_client_protocol::Responder<LogoutResponse>,
    ) -> agent_client_protocol::Result<()> {
        self.provider.write().await.enabled = false;
        responder.respond(LogoutResponse::new())
    }

    async fn list_providers(
        self,
        _request: ListProvidersRequest,
        responder: agent_client_protocol::Responder<ListProvidersResponse>,
    ) -> agent_client_protocol::Result<()> {
        let routing = self.provider.read().await;
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
                Error::invalid_params().data("DeepSeek requires openai protocol"),
            );
        }
        if reqwest::Url::parse(&request.base_url).is_err() {
            return responder
                .respond_with_error(Error::invalid_params().data("provider base URL is invalid"));
        }
        *self.provider.write().await = ProviderRouting {
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
        self.provider.write().await.enabled = false;
        responder.respond(DisableProviderResponse::new())
    }

    async fn new_session(
        self,
        request: NewSessionRequest,
        responder: agent_client_protocol::Responder<NewSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        if !self.initialized.load(Ordering::Acquire) {
            return responder.respond_with_error(Error::invalid_request().data("initialize first"));
        }
        let system_prompt = match client_system_prompt(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt,
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self
            .store
            .create(
                request.cwd,
                request.additional_directories,
                system_prompt,
                None,
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            let _ = self.store.delete(&runtime.snapshot().await.id).await;
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        responder.respond(
            NewSessionResponse::new(record.id.clone())
                .modes(Some(mode_state(&record)))
                .config_options(Some(self.config_options(&record))),
        )?;
        send_available_commands(&connection, &record.id)
    }

    async fn load_session(
        self,
        request: LoadSessionRequest,
        responder: agent_client_protocol::Responder<LoadSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let system_prompt = match client_system_prompt(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt,
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        if runtime
            .state
            .lock()
            .await
            .active_prompt
            .load(Ordering::Acquire)
        {
            return responder.respond_with_error(
                Error::invalid_request().data("cannot load a session with active work"),
            );
        }
        if session::validate_roots(&request.cwd, &request.additional_directories).is_err() {
            return responder.respond_with_error(
                Error::invalid_params().data("invalid session workspace roots"),
            );
        }
        let instructions = {
            let state = runtime.state.lock().await;
            if state.record.cwd != request.cwd {
                return responder.respond_with_error(Error::invalid_params().data(format!(
                    "session cwd mismatch: stored {}, requested {}",
                    state.record.cwd.display(),
                    request.cwd.display()
                )));
            }
            match self.store.session_instructions(
                &state.record,
                request.additional_directories,
                system_prompt,
            ) {
                Ok(instructions) => instructions,
                Err(error) => return responder.respond_with_error(error),
            }
        };
        // A load whose MCP servers fail to connect leaves the session as it was.
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            return responder.respond_with_error(error);
        }
        instructions.install(&mut runtime.state.lock().await.record);
        if let Err(error) = self.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        let replay_compactions = self
            .client_capabilities
            .read()
            .await
            .session
            .as_ref()
            .and_then(|session| session.compaction.as_ref())
            .is_some();
        if let Err(error) = replay_history(&connection, &record, replay_compactions) {
            return responder.respond_with_error(error);
        }
        responder.respond(
            LoadSessionResponse::new()
                .modes(Some(mode_state(&record)))
                .config_options(Some(self.config_options(&record))),
        )?;
        send_available_commands(&connection, &record.id)
    }

    async fn resume_session(
        self,
        request: ResumeSessionRequest,
        responder: agent_client_protocol::Responder<ResumeSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let system_prompt = match client_system_prompt(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt,
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        if runtime
            .state
            .lock()
            .await
            .active_prompt
            .load(Ordering::Acquire)
        {
            return responder.respond_with_error(
                Error::invalid_request().data("cannot resume a session with active work"),
            );
        }
        let instructions = {
            let state = runtime.state.lock().await;
            if state.record.cwd != request.cwd {
                return responder.respond_with_error(Error::invalid_params().data(format!(
                    "session cwd mismatch: stored {}, requested {}",
                    state.record.cwd.display(),
                    request.cwd.display()
                )));
            }
            if let Err(error) =
                crate::session::validate_roots(&request.cwd, &request.additional_directories)
            {
                return responder.respond_with_error(error);
            }
            match self.store.session_instructions(
                &state.record,
                request.additional_directories,
                system_prompt,
            ) {
                Ok(instructions) => instructions,
                Err(error) => return responder.respond_with_error(error),
            }
        };
        // A resume whose MCP servers fail to connect leaves the session as it
        // was.
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            return responder.respond_with_error(error);
        }
        instructions.install(&mut runtime.state.lock().await.record);
        if let Err(error) = self.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        responder.respond(
            ResumeSessionResponse::new()
                .modes(Some(mode_state(&record)))
                .config_options(Some(self.config_options(&record))),
        )?;
        send_available_commands(&connection, &record.id)
    }

    async fn fork_session(
        self,
        request: ForkSessionRequest,
        responder: agent_client_protocol::Responder<ForkSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let source_runtime = match self.store.get(&request.session_id.to_string()).await {
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
        if source.cwd != request.cwd {
            return responder.respond_with_error(Error::invalid_params().data(format!(
                "session cwd mismatch: stored {}, requested {}",
                source.cwd.display(),
                request.cwd.display()
            )));
        }
        let system_prompt = match client_system_prompt(request.system_prompt.as_ref()) {
            Ok(prompt) => prompt.or_else(|| source.client_system_prompt()),
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime = match self
            .store
            .create(
                request.cwd,
                request.additional_directories,
                system_prompt,
                Some(source.id.clone()),
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let runtime_id = runtime.snapshot().await.id;
        let refresh = {
            let mut state = runtime.state.lock().await;
            let mut candidate = state.record.clone();
            candidate.messages = source.messages.clone();
            candidate.prompt_blocks = source.prompt_blocks.clone();
            candidate.message_ids = source.message_ids.clone();
            candidate.pending_steering = source.pending_steering.clone();
            candidate.tool_outcomes = source.tool_outcomes.clone();
            candidate.compactions = source.compactions.clone();
            candidate.config = source.config.clone();
            candidate.title = source.title.clone();
            candidate.usage = source.usage.clone();
            candidate.context_used = source.context_used;
            let refresh = self.store.rebuild_system_prompt(&mut candidate);
            if refresh.is_ok() {
                state.record = candidate;
            }
            refresh
        };
        if let Err(error) = refresh {
            let _ = self.store.delete(&runtime_id).await;
            return responder.respond_with_error(error);
        }
        if let Err(error) = self.attach_mcp(&runtime, &request.mcp_servers).await {
            let _ = self.store.delete(&runtime_id).await;
            return responder.respond_with_error(error);
        }
        if let Err(error) = self.store.save(&runtime).await {
            let _ = self.store.delete(&runtime_id).await;
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        responder.respond(
            ForkSessionResponse::new(record.id.clone())
                .modes(Some(mode_state(&record)))
                .config_options(Some(self.config_options(&record))),
        )?;
        send_available_commands(&connection, &record.id)
    }

    async fn list_sessions(
        self,
        request: ListSessionsRequest,
        responder: agent_client_protocol::Responder<ListSessionsResponse>,
    ) -> agent_client_protocol::Result<()> {
        if request.cwd.as_ref().is_some_and(|cwd| !cwd.is_absolute()) {
            return responder.respond_with_error(
                Error::invalid_params().data("session/list cwd must be absolute"),
            );
        }
        let offset = match request.cursor.as_deref() {
            Some(cursor) => match cursor.parse::<usize>() {
                Ok(offset) => offset,
                Err(_) => {
                    return responder.respond_with_error(
                        Error::invalid_params().data("invalid pagination cursor"),
                    );
                }
            },
            None => 0,
        };
        let records = match self.store.list(request.cwd.as_deref()).await {
            Ok(records) => records,
            Err(error) => return responder.respond_with_error(error),
        };
        let total = records.len();
        let sessions = records
            .into_iter()
            .skip(offset)
            .take(PAGE_SIZE)
            .map(session_info)
            .collect::<Vec<_>>();
        let next = offset.saturating_add(sessions.len());
        responder.respond(
            ListSessionsResponse::new(sessions)
                .next_cursor((next < total).then(|| next.to_string())),
        )
    }

    async fn delete_session(
        self,
        request: DeleteSessionRequest,
        responder: agent_client_protocol::Responder<DeleteSessionResponse>,
    ) -> agent_client_protocol::Result<()> {
        match self.store.delete(&request.session_id.to_string()).await {
            Ok(()) => responder.respond(DeleteSessionResponse::new()),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn close_session(
        self,
        request: CloseSessionRequest,
        responder: agent_client_protocol::Responder<CloseSessionResponse>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.store.get(&request.session_id.to_string()).await {
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
        match self.store.close(&request.session_id.to_string()).await {
            Ok(()) => responder.respond(CloseSessionResponse::new()),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn session_status(
        self,
        params: Value,
        responder: agent_client_protocol::Responder<Value>,
    ) -> agent_client_protocol::Result<()> {
        if !self.initialized.load(Ordering::Acquire) {
            return responder.respond_with_error(Error::invalid_request().data("initialize first"));
        }
        let session_id = match session_status::requested_session_id(&params) {
            Ok(session_id) => session_id,
            Err(error) => return responder.respond_with_error(error),
        };
        // v1 has no foreground state model, so the response has no `state`.
        match self.store.presence(session_id).await {
            Ok(SessionPresence::Active(_)) => responder.respond(json!({"status": "active"})),
            Ok(SessionPresence::Inactive) => responder.respond(json!({"status": "inactive"})),
            Ok(SessionPresence::Unknown) => {
                responder.respond_with_error(session_status::unknown_session(session_id))
            }
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn set_session_mode(
        self,
        request: SetSessionModeRequest,
        responder: agent_client_protocol::Responder<SetSessionModeResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let mode = request.mode_id.to_string();
        if !matches!(mode.as_str(), "agent" | "plan") {
            return responder.respond_with_error(
                Error::invalid_params().data(format!("unknown session mode: {mode}")),
            );
        }
        let runtime = match self.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        {
            let mut state = runtime.state.lock().await;
            state.record.config.mode = mode.clone();
            state.record.touch();
        }
        if let Err(error) = self.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        responder.respond(SetSessionModeResponse::new())?;
        connection.send_notification(SessionNotification::new(
            request.session_id,
            agent_client_protocol::schema::v1::SessionUpdate::CurrentModeUpdate(
                CurrentModeUpdate::new(mode),
            ),
        ))?;
        connection.send_notification(SessionNotification::new(
            SessionId::new(runtime.snapshot().await.id),
            agent_client_protocol::schema::v1::SessionUpdate::ConfigOptionUpdate(
                ConfigOptionUpdate::new(self.config_options(&runtime.snapshot().await)),
            ),
        ))
    }

    async fn set_session_config(
        self,
        request: SetSessionConfigOptionRequest,
        responder: agent_client_protocol::Responder<SetSessionConfigOptionResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let config_id = request.config_id.to_string();
        let value = request.value;
        let mut state = runtime.state.lock().await;
        let apply = apply_config_value(&mut state.record, &config_id, &value, &self.config.models);
        if let Err(error) = apply {
            return responder.respond_with_error(error);
        }
        state.record.touch();
        drop(state);
        if let Err(error) = self.store.save(&runtime).await {
            return responder.respond_with_error(error);
        }
        let record = runtime.snapshot().await;
        let options = self.config_options(&record);
        connection.send_notification(SessionNotification::new(
            request.session_id.clone(),
            agent_client_protocol::schema::v1::SessionUpdate::ConfigOptionUpdate(
                ConfigOptionUpdate::new(options.clone()),
            ),
        ))?;
        responder.respond(SetSessionConfigOptionResponse::new(options))
    }

    async fn prompt(
        self,
        request: PromptRequest,
        responder: agent_client_protocol::Responder<PromptResponse>,
        connection: ConnectionTo<Client>,
    ) -> agent_client_protocol::Result<()> {
        let runtime = match self.store.get(&request.session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let cancellation = CancellationToken::new();
        {
            let mut state = runtime.state.lock().await;
            if state.active_prompt.load(Ordering::Acquire) {
                return responder.respond_with_error(
                    Error::invalid_request().data("a prompt is already active for this session"),
                );
            }
            state.active_prompt.store(true, Ordering::Release);
            state.cancellation = cancellation.clone();
        }
        let client_capabilities = self.client_capabilities.read().await.clone();
        let harness = self.clone();
        let prompt_connection = connection.clone();
        let active_prompt = runtime.state.lock().await.active_prompt.clone();
        let request_cancellation = responder.cancellation();
        let spawn_result = connection.spawn(async move {
            // `$/cancel_request` stops the turn the way `session/cancel` does,
            // so tool calls still get their final state and are saved. It
            // cancels only this child token, so the session token still shows
            // whether session/cancel or a close reached the turn.
            let turn_cancellation = cancellation.child_token();
            let turn = harness.run_prompt(
                runtime.clone(),
                request,
                prompt_connection,
                client_capabilities,
                turn_cancellation.clone(),
            );
            tokio::pin!(turn);
            let result = tokio::select! {
                biased;
                result = &mut turn => result,
                () = request_cancellation.cancelled() => {
                    turn_cancellation.cancel();
                    turn.await
                }
            };
            // A prompt that session/cancel reached must end with `cancelled`,
            // which `$/cancel_request` accepts too. A turn stopped by
            // `$/cancel_request` alone answers -32800, and once the request
            // is cancelled no other error code is valid.
            let cancelled_by_request = request_cancellation.is_cancelled();
            let stopped_by_request = cancelled_by_request && !cancellation.is_cancelled();
            match result {
                Ok(response)
                    if stopped_by_request && response.stop_reason == StopReason::Cancelled =>
                {
                    responder.respond_with_error(Error::request_cancelled())
                }
                Ok(response) => responder.respond(response),
                Err(error)
                    if error.code == agent_client_protocol::ErrorCode::RequestCancelled
                        && !stopped_by_request =>
                {
                    responder.respond(PromptResponse::new(StopReason::Cancelled))
                }
                Err(error) if cancelled_by_request => {
                    if error.code != agent_client_protocol::ErrorCode::RequestCancelled {
                        tracing::warn!(?error, "cancelled prompt turn failed");
                    }
                    responder.respond_with_error(Error::request_cancelled())
                }
                Err(error) => responder.respond_with_error(error),
            }
        });
        if let Err(error) = spawn_result {
            active_prompt.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    async fn steering(
        self,
        params: Value,
        responder: agent_client_protocol::Responder<Value>,
        connection: ConnectionTo<Client>,
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
        let session_id = SessionId::new(session_id.to_string());
        let runtime = match self.store.get(&session_id.to_string()).await {
            Ok(runtime) => runtime,
            Err(error) => return responder.respond_with_error(error),
        };
        let lifecycle = runtime.lifecycle.lock().await;
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
            && !Config::model_supports_images(&model)
        {
            return responder.respond_with_error(Error::invalid_params().data(format!(
                "model {model} does not support image prompts; select deepseek-flash"
            )));
        }
        if !is_active {
            let cancellation = CancellationToken::new();
            {
                let mut state = runtime.state.lock().await;
                state.cancellation = cancellation.clone();
                state.prompt_seq = state.prompt_seq.saturating_add(1);
            }
            let request = PromptRequest::new(session_id.clone(), prompt);
            let work_runtime = runtime.clone();
            let work_connection = connection.clone();
            let capabilities = self.client_capabilities.read().await.clone();
            let spawn_result = connection.spawn(async move {
                let result = self
                    .run_prompt(
                        work_runtime,
                        request,
                        work_connection,
                        capabilities,
                        cancellation,
                    )
                    .await;
                if let Err(error) = result {
                    tracing::warn!(?error, "detached steering turn failed");
                }
                Ok(())
            });
            drop(turn);
            drop(lifecycle);
            if let Err(error) = spawn_result {
                return responder.respond_with_error(error);
            }
            return responder.respond(json!({"outcome": "startedNewTurn"}));
        }

        let blocks = serde_json::to_value(&prompt)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        let (message_id, previous_index, previous_seq) = {
            let mut state = runtime.state.lock().await;
            let previous_index = state.record.pending_steering.len();
            let previous_seq = state.prompt_seq;
            state.prompt_seq = state.prompt_seq.saturating_add(1);
            let message_id = format!("steer-{}", uuid::Uuid::new_v4().simple());
            state.record.pending_steering.push(PendingUserMessage {
                message_id: message_id.clone(),
                content: payload.content,
                blocks,
            });
            state.record.touch();
            (message_id, previous_index, previous_seq)
        };
        if let Err(error) = self.store.save(&runtime).await {
            let mut state = runtime.state.lock().await;
            state.record.pending_steering.truncate(previous_index);
            state.prompt_seq = previous_seq;
            return responder.respond_with_error(error);
        }
        for block in prompt {
            connection.send_notification(SessionNotification::new(
                session_id.clone(),
                agent_client_protocol::schema::v1::SessionUpdate::UserMessageChunk(
                    ContentChunk::new(block).message_id(
                        agent_client_protocol::schema::v1::MessageId::new(message_id.clone()),
                    ),
                ),
            ))?;
        }
        responder.respond(json!({"outcome": "injected"}))
    }

    async fn cancel(&self, notification: CancelNotification) {
        if let Ok(runtime) = self.store.get(&notification.session_id.to_string()).await {
            runtime.state.lock().await.cancellation.cancel();
        }
    }

    pub(crate) async fn attach_mcp(
        &self,
        runtime: &SessionRuntime,
        servers: &[agent_client_protocol::schema::v1::McpServer],
    ) -> Result<(), Error> {
        if servers.is_empty() {
            runtime.state.lock().await.mcp = None;
            return Ok(());
        }
        // Connecting belongs to this lifecycle request, not to a turn. The
        // session token is the last turn's, which stays cancelled after
        // `session/cancel` until the next turn replaces it. Nothing cancels
        // this one, so the connect timeout is what bounds the wait.
        let mcp = McpSession::connect(
            servers,
            self.config.mcp_connect_timeout,
            &CancellationToken::new(),
        )
        .await?;
        runtime.state.lock().await.mcp = Some(Arc::new(mcp));
        Ok(())
    }

    async fn run_prompt(
        self,
        runtime: SessionRuntime,
        request: PromptRequest,
        connection: ConnectionTo<Client>,
        client_capabilities: ClientCapabilities,
        cancellation: CancellationToken,
    ) -> Result<PromptResponse, Error> {
        let lifecycle = runtime.lifecycle.lock().await;
        let _guard = {
            let state = runtime.state.lock().await;
            state.active_prompt.store(true, Ordering::Release);
            PromptGuard {
                active: state.active_prompt.clone(),
                notify: state.activity_notify.clone(),
            }
        };
        if runtime.closed.load(Ordering::Acquire) {
            return Err(Error::request_cancelled());
        }
        let session_id = request.session_id.clone();
        let prompt = content_blocks_to_prompt(&request.prompt)?;
        if prompt.text.trim().is_empty() && !prompt.has_images {
            return Err(Error::invalid_params().data("prompt contains no usable content"));
        }

        if let Some(command_response) = self
            .handle_command(&runtime, &connection, &session_id, &prompt.text)
            .await?
        {
            return Ok(command_response);
        }

        let initial_record = runtime.snapshot().await;
        if prompt.has_images && !Config::model_supports_images(&initial_record.config.model) {
            return Err(Error::invalid_params().data(format!(
                "model {} does not support image prompts; select deepseek-flash",
                initial_record.config.model
            )));
        }
        let (message_index, previous_title) = {
            let mut state = runtime.state.lock().await;
            let message_index = state.record.messages.len();
            let previous_title = state.record.title.clone();
            state
                .record
                .messages
                .push(ChatMessage::user_content(prompt.content));
            if let Ok(blocks) = serde_json::to_value(&request.prompt)
                && let Some(blocks) = blocks.as_array()
            {
                let blocks = blocks.clone();
                state
                    .record
                    .prompt_blocks
                    .insert(message_index.to_string(), blocks);
            }
            let replay_id = format!("history-message-{message_index}");
            state
                .record
                .message_ids
                .insert(message_index.to_string(), replay_id);
            state.record.touch();
            if state.record.title.is_none() {
                state.record.title = session_title(&state.record);
            }
            (message_index, previous_title)
        };
        if let Err(error) = self.store.save(&runtime).await {
            let mut state = runtime.state.lock().await;
            if state.record.messages.len() > message_index {
                state.record.messages.remove(message_index);
            }
            state
                .record
                .prompt_blocks
                .remove(&message_index.to_string());
            state.record.message_ids.remove(&message_index.to_string());
            state.record.title = previous_title;
            return Err(error);
        }
        drop(lifecycle);
        let record = runtime.snapshot().await;
        connection.send_notification(SessionNotification::new(
            session_id.clone(),
            agent_client_protocol::schema::v1::SessionUpdate::SessionInfoUpdate(
                SessionInfoUpdate::new().title(record.title.clone()),
            ),
        ))?;
        send_usage_update(&connection, &record)?;

        let deepseek = self.deepseek_client().await?;
        let supports_elicitation =
            client_capabilities
                .elicitation
                .as_ref()
                .is_some_and(|capabilities| {
                    capabilities.supports_form() || capabilities.supports_url()
                });
        let supports_url_elicitation = client_capabilities
            .elicitation
            .as_ref()
            .is_some_and(|capabilities| capabilities.supports_url());
        let access = ToolAccess {
            connection: Some(connection.clone()),
            client: client_capabilities,
            config: self.config.clone(),
        };
        let mut final_usage = initial_record.usage;
        // ACP v1 defines the prompt response's usage as this turn's; the record
        // keeps the session total for usage_update.
        let mut turn_usage = TokenUsage::default();
        let supports_compaction = self
            .client_capabilities
            .read()
            .await
            .session
            .as_ref()
            .and_then(|session| session.compaction.as_ref())
            .is_some();
        let supports_notices = self
            .client_capabilities
            .read()
            .await
            .session
            .as_ref()
            .and_then(|session| session.notices.as_ref())
            .is_some();
        let mut compaction_reported = false;

        // A turn runs until the model stops asking for tools, the user
        // cancels, or a request fails. There is no round limit: a task that
        // needs many steps used to be cut off mid-way.
        loop {
            let flushed_steering = {
                let mut state = runtime.state.lock().await;
                let pending = !state.record.pending_steering.is_empty();
                if pending {
                    state.record.flush_pending_steering();
                    state.record.touch();
                }
                pending
            };
            if flushed_steering {
                self.store.save(&runtime).await?;
            }
            if cancellation.is_cancelled() {
                return Ok(PromptResponse::new(StopReason::Cancelled));
            }
            let (record, request_seq) = {
                let state = runtime.state.lock().await;
                (state.record.clone(), state.prompt_seq)
            };
            let mut tools = built_in_definitions(&self.config);
            if supports_elicitation {
                tools.push(crate::tools::ask_user_definition());
            }
            if supports_url_elicitation {
                tools.push(crate::tools::complete_url_elicitation_definition());
            }
            let read_only_mcp_names = {
                let mcp = runtime.state.lock().await.mcp.clone();
                if let Some(mcp) = mcp {
                    if let Err(error) = mcp.refresh(&cancellation).await {
                        // A cancel that lands during the refresh ends the
                        // turn; the refresh did not fail.
                        if error.code == agent_client_protocol::ErrorCode::RequestCancelled {
                            return Ok(PromptResponse::new(StopReason::Cancelled));
                        }
                        tracing::warn!(?error, "failed to refresh MCP tools");
                        if supports_notices {
                            let _ = connection.send_notification(SessionNotification::new(
                                session_id.clone(),
                                agent_client_protocol::schema::v1::SessionUpdate::Notice(
                                    Notice::new(NoticeSeverity::Warning, "MCP tool refresh failed")
                                        .description(Some(error.message)),
                                ),
                            ));
                        }
                    }
                    tools.extend(mcp.definitions());
                    mcp.read_only_names()
                } else {
                    Vec::new()
                }
            };
            tools.truncate(MAX_MODEL_TOOLS);
            let mut context = record.context_messages(self.config.context_chars);
            if record.config.mode == "plan" {
                crate::tools::restrict_to_plan_mode(&mut tools, &mut context, &read_only_mcp_names);
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
            if supports_compaction
                && !compaction_reported
                && retained_non_system < original_non_system
            {
                let compaction_id = format!("cmp-{}", uuid::Uuid::new_v4().simple());
                {
                    let mut state = runtime.state.lock().await;
                    state.record.compactions.push(PersistedCompaction {
                        id: compaction_id.clone(),
                        status: "completed".to_string(),
                    });
                    state.record.touch();
                }
                self.store.save(&runtime).await?;
                connection.send_notification(SessionNotification::new(
                    session_id.clone(),
                    agent_client_protocol::schema::v1::SessionUpdate::CompactionUpdate(
                        CompactionUpdate::new(compaction_id, CompactionStatus::Completed),
                    ),
                ))?;
                compaction_reported = true;
            }
            let request = deepseek.request(
                record.config.model.clone(),
                record.config.reasoning_effort,
                context,
                tools,
            );
            let thought_message_id = format!("thought-{}", uuid::Uuid::new_v4().simple());
            let answer_message_id = format!("message-{}", uuid::Uuid::new_v4().simple());
            let mut on_event = |event: StreamEvent| {
                if let Err(error) = match event {
                    StreamEvent::Text(text) => {
                        send_agent_chunk(&connection, &session_id, &answer_message_id, text, false)
                    }
                    StreamEvent::Reasoning(text) => {
                        send_agent_chunk(&connection, &session_id, &thought_message_id, text, true)
                    }
                    StreamEvent::Usage(_) => Ok(()),
                } {
                    tracing::warn!(?error, "failed to send ACP stream update");
                }
            };
            let mut completion = match deepseek
                .stream_chat(request, &cancellation, &mut on_event)
                .await
            {
                Ok(completion) => crate::deepseek::sanitize_tool_calls(completion),
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
                    return Ok(PromptResponse::new(StopReason::Cancelled));
                }
                Err(error) => return Err(error),
            };
            ensure_session_unique_tool_call_ids(
                &runtime.snapshot().await,
                &mut completion.tool_calls,
            );
            final_usage.saturating_add(&completion.usage);
            turn_usage.saturating_add(&completion.usage);
            self.persist_completion(&runtime, &completion, final_usage.clone())
                .await?;
            send_usage_update(&connection, &runtime.snapshot().await)?;

            // A response cut off by the token cap is not a finished answer. The
            // tool calls it carries are half-written (their arguments are
            // truncated), so they must not run, and the turn must not end
            // silently.
            if completion.finish_reason.as_deref() == Some("length") {
                {
                    let mut state = runtime.state.lock().await;
                    state.record.flush_pending_steering();
                    state.record.touch();
                }
                self.store.save(&runtime).await?;
                if !completion.tool_calls.is_empty() {
                    self.append_unfinished_tool_results(
                        &runtime,
                        &access,
                        &session_id,
                        &completion.tool_calls,
                        0,
                        "Tool call was not executed because the response was cut off at the token limit.",
                    )
                    .await?;
                }
                if supports_notices {
                    let _ = connection.send_notification(SessionNotification::new(
                        session_id.clone(),
                        agent_client_protocol::schema::v1::SessionUpdate::Notice(
                            Notice::new(
                                NoticeSeverity::Warning,
                                "The response reached the token limit and was cut off",
                            )
                            .description(Some(
                                "Raise --max-tokens, or omit it to use the model's maximum."
                                    .to_string(),
                            )),
                        ),
                    ));
                }
                return Ok(PromptResponse::new(StopReason::MaxTokens)
                    .usage(Some(turn_acp_usage(turn_usage))));
            }

            if completion.tool_calls.is_empty() {
                let steering_pending = {
                    let mut state = runtime.state.lock().await;
                    let pending =
                        state.prompt_seq > request_seq || !state.record.pending_steering.is_empty();
                    if pending {
                        state.record.flush_pending_steering();
                        state.record.touch();
                    }
                    pending
                };
                if steering_pending {
                    self.store.save(&runtime).await?;
                    continue;
                }
                let stop_reason = match completion.finish_reason.as_deref() {
                    Some("content_filter") => StopReason::Refusal,
                    _ => StopReason::EndTurn,
                };
                return Ok(PromptResponse::new(stop_reason).usage(Some(turn_acp_usage(turn_usage))));
            }

            for (index, tool_call) in completion.tool_calls.iter().enumerate() {
                if cancellation.is_cancelled() {
                    self.append_unfinished_tool_results(
                        &runtime,
                        &access,
                        &session_id,
                        &completion.tool_calls,
                        index,
                        "Tool call was cancelled before execution.",
                    )
                    .await?;
                    return Ok(PromptResponse::new(StopReason::Cancelled));
                }
                let result = match self
                    .execute_model_tool(&runtime, &access, &session_id, tool_call, &cancellation)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        self.append_unfinished_tool_results(
                            &runtime,
                            &access,
                            &session_id,
                            &completion.tool_calls,
                            index,
                            crate::model::INTERRUPTED_TOOL_CALL,
                        )
                        .await?;
                        if error.code == agent_client_protocol::ErrorCode::RequestCancelled {
                            return Ok(PromptResponse::new(StopReason::Cancelled));
                        }
                        return Err(error);
                    }
                };
                let mut state = runtime.state.lock().await;
                let outcome = persisted_tool_outcome(tool_call, &result, result_status(&result));
                state
                    .record
                    .tool_outcomes
                    .insert(tool_call.id.clone(), outcome);
                state
                    .record
                    .messages
                    .push(ChatMessage::tool(tool_call.id.clone(), result.text));
                state.record.touch();
                drop(state);
                self.store.save(&runtime).await?;
            }
        }
    }

    async fn append_unfinished_tool_results(
        &self,
        runtime: &SessionRuntime,
        access: &ToolAccess,
        session_id: &SessionId,
        calls: &[crate::model::ToolCall],
        start: usize,
        reason: &str,
    ) -> Result<(), Error> {
        {
            let mut state = runtime.state.lock().await;
            for call in calls.iter().skip(start) {
                let fields = ToolCallUpdateFields::new().status(Some(ToolCallStatus::Failed));
                access
                    .connection
                    .as_ref()
                    .expect("v1 tool cancellation requires an ACP v1 connection")
                    .send_notification(SessionNotification::new(
                        session_id.clone(),
                        agent_client_protocol::schema::v1::SessionUpdate::ToolCallUpdate(
                            ToolCallUpdate::new(call.id.clone(), fields),
                        ),
                    ))?;
                let result = ToolResult {
                    text: reason.to_string(),
                    raw: json!({"status": "failed"}),
                };
                state.record.tool_outcomes.insert(
                    call.id.clone(),
                    persisted_tool_outcome(call, &result, ToolOutcomeStatus::Failed),
                );
                state
                    .record
                    .messages
                    .push(ChatMessage::tool(call.id.clone(), reason));
            }
            state.record.touch();
        }
        self.store.save(runtime).await
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
        self.store.save(runtime).await?;
        Ok(())
    }

    async fn execute_model_tool(
        &self,
        runtime: &SessionRuntime,
        access: &ToolAccess,
        session_id: &SessionId,
        call: &crate::model::ToolCall,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, Error> {
        let arguments: Value = match serde_json::from_str(&call.function.arguments) {
            Ok(arguments) => arguments,
            Err(error) => {
                let message = format!(
                    "Tool {} received invalid JSON arguments: {error}",
                    call.function.name
                );
                let result = ToolResult {
                    text: message.clone(),
                    raw: json!({"error": message}),
                };
                access
                    .connection
                    .as_ref()
                    .expect("v1 tool update requires an ACP v1 connection")
                    .send_notification(SessionNotification::new(
                        session_id.clone(),
                        agent_client_protocol::schema::v1::SessionUpdate::ToolCall(
                            ToolCall::new(call.id.clone(), call.function.name.clone())
                                .name(call.function.name.clone())
                                .kind(tool_kind(&call.function.name))
                                .status(ToolCallStatus::Failed)
                                .raw_input(Value::String(call.function.arguments.clone()))
                                .content(vec![ToolCallContent::Content(
                                    agent_client_protocol::schema::v1::Content::new(
                                        ContentBlock::Text(TextContent::new(&message)),
                                    ),
                                )]),
                        ),
                    ))?;
                return Ok(result);
            }
        };
        let kind = tool_kind(&call.function.name);
        let title = tool_title(&call.function.name, &arguments);
        let record = runtime.snapshot().await;
        let locations = tool_locations(&call.function.name, &arguments, &record);
        let read_only_mcp = runtime
            .state
            .lock()
            .await
            .mcp
            .as_ref()
            .is_some_and(|mcp| mcp.is_read_only(&call.function.name));
        let refused_in_plan_mode = record.config.mode == "plan"
            && crate::tools::refused_in_plan_mode(&call.function.name, read_only_mcp);
        // Show what a write or edit will change before the user approves it.
        // Plan mode refuses the call below, so there is nothing to show.
        let preview = if refused_in_plan_mode {
            None
        } else {
            crate::tools::preview_diff(&record, &call.function.name, &arguments).await
        };
        access
            .connection
            .as_ref()
            .expect("v1 tool update requires an ACP v1 connection")
            .send_notification(SessionNotification::new(
                session_id.clone(),
                agent_client_protocol::schema::v1::SessionUpdate::ToolCall(
                    ToolCall::new(call.id.clone(), title.clone())
                        .name(call.function.name.clone())
                        .kind(kind)
                        .status(ToolCallStatus::Pending)
                        .locations(locations)
                        .content(preview.iter().map(diff_content).collect())
                        .raw_input(arguments.clone()),
                ),
            ))?;

        // Refuse before asking permission: approving a call plan mode then
        // refuses would only mislead the user.
        if refused_in_plan_mode {
            let result = ToolResult {
                text: crate::tools::PLAN_MODE_REFUSAL.to_string(),
                raw: json!({"denied": true, "reason": "plan_mode"}),
            };
            send_tool_finished(access, session_id, call, &title, &result, false, None)?;
            return Ok(result);
        }

        if call.function.name == "update_plan" {
            let plan = match parse_plan(&arguments) {
                Ok(plan) => plan,
                Err(error) => {
                    let result = ToolResult {
                        text: format!("Unable to update plan: {}", error.message),
                        raw: json!({"error": error.message}),
                    };
                    send_tool_finished(access, session_id, call, &title, &result, false, None)?;
                    return Ok(result);
                }
            };
            access
                .connection
                .as_ref()
                .expect("v1 tool update requires an ACP v1 connection")
                .send_notification(SessionNotification::new(
                    session_id.clone(),
                    agent_client_protocol::schema::v1::SessionUpdate::Plan(plan.clone()),
                ))?;
            let result = ToolResult {
                text: format!(
                    "Plan updated with {} entr{}.",
                    plan.entries.len(),
                    if plan.entries.len() == 1 { "y" } else { "ies" }
                ),
                raw: json!({"entries": plan.entries.len()}),
            };
            send_tool_finished(access, session_id, call, &title, &result, true, None)?;
            return Ok(result);
        }

        if call.function.name == "ask_user" {
            let message = match arguments.get("message").and_then(Value::as_str) {
                Some(message) => message,
                None => {
                    let result = ToolResult {
                        text: "ask_user requires a message.".to_string(),
                        raw: json!({"error": "missing message"}),
                    };
                    send_tool_finished(access, session_id, call, &title, &result, false, None)?;
                    return Ok(result);
                }
            };
            let (result, success) = match ask_user(
                access,
                &self.store,
                session_id,
                call,
                message,
                &arguments,
                cancellation,
            )
            .await
            {
                Ok(result) => (result, true),
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
                    return Err(error);
                }
                Err(error) => (
                    crate::tools::error_result("Unable to request user input", &error),
                    false,
                ),
            };
            send_tool_finished(access, session_id, call, &title, &result, success, None)?;
            return Ok(result);
        }

        // Only a protocol notification to the Client, so it needs no
        // permission and stays available in plan mode.
        if call.function.name == "complete_url_elicitation" {
            let connection = access
                .connection
                .as_ref()
                .expect("v1 elicitation completion requires an ACP v1 connection");
            let result = crate::tools::complete_url_elicitation(
                &self.store,
                &session_id.to_string(),
                &arguments,
                |elicitation_id| {
                    connection.send_notification(
                        agent_client_protocol::schema::v1::CompleteElicitationNotification::new(
                            elicitation_id,
                        ),
                    )
                },
            )
            .await;
            let success = result_status(&result) == ToolOutcomeStatus::Completed;
            send_tool_finished(access, session_id, call, &title, &result, success, None)?;
            return Ok(result);
        }

        let needs_permission = crate::tools::permission_required(&call.function.name)
            || call.function.name.starts_with("mcp__");
        if needs_permission && !cancellation.is_cancelled() {
            let record = runtime.snapshot().await;
            let approval_key =
                crate::tools::session_approval_key(&record, &call.function.name, &arguments);
            let approved = runtime.state.lock().await.session_approvals.clone();
            if !crate::tools::is_auto_approved(&record, &approved, &approval_key) {
                if record.config.permission_mode == PermissionMode::Deny {
                    let denied = ToolResult {
                        text: "Permission denied by the configured permission policy.".to_string(),
                        raw: json!({"denied": true}),
                    };
                    send_tool_finished(access, session_id, call, &title, &denied, false, None)?;
                    return Ok(denied);
                }
                match request_permission(
                    access,
                    session_id,
                    call,
                    &title,
                    &arguments,
                    preview.as_ref(),
                    cancellation,
                )
                .await?
                {
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
                        let denied = ToolResult {
                            text: "The user denied permission for this tool call.".to_string(),
                            raw: json!({"denied": true}),
                        };
                        send_tool_finished(access, session_id, call, &title, &denied, false, None)?;
                        return Ok(denied);
                    }
                    PermissionDecision::Cancelled => {
                        return Err(Error::request_cancelled());
                    }
                }
            }
        }

        access
            .connection
            .as_ref()
            .expect("v1 tool result requires an ACP v1 connection")
            .send_notification(SessionNotification::new(
                session_id.clone(),
                agent_client_protocol::schema::v1::SessionUpdate::ToolCallUpdate(
                    ToolCallUpdate::new(
                        call.id.clone(),
                        ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
                    ),
                ),
            ))?;

        let terminal_call = TerminalToolCall::new(call.id.clone());
        let result = if call.function.name.starts_with("mcp__") {
            let mcp_result = {
                let Some(mcp) = runtime.state.lock().await.mcp.clone() else {
                    return Err(Error::internal_error().data("MCP session is unavailable"));
                };
                mcp.call(&call.function.name, arguments, cancellation).await
            };
            match mcp_result {
                Ok(value) => ToolResult {
                    text: truncate_text(&mcp_result_text(&value), self.config.tool_output_bytes),
                    raw: value,
                },
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
                    return Err(error);
                }
                Err(error) => {
                    let message = error.message.clone();
                    ToolResult {
                        text: format!(
                            "MCP tool failed: {}",
                            error.data.unwrap_or_else(|| Value::String(message.clone()))
                        ),
                        raw: json!({"error": message}),
                    }
                }
            }
        } else {
            match execute_tool(
                access,
                &runtime.snapshot().await,
                &call.function.name,
                arguments,
                Some(&terminal_call),
                cancellation,
            )
            .await
            {
                Ok(result) => result,
                Err(error) if error.code == agent_client_protocol::ErrorCode::RequestCancelled => {
                    return Err(error);
                }
                Err(error) => {
                    let message = error.message.clone();
                    ToolResult {
                        text: format!(
                            "Tool failed: {}",
                            error.data.unwrap_or_else(|| Value::String(message.clone()))
                        ),
                        raw: json!({"error": message}),
                    }
                }
            }
        };
        let success = result.raw.get("error").is_none()
            && result
                .raw
                .get("isError")
                .and_then(Value::as_bool)
                .is_none_or(|is_error| !is_error);
        send_tool_finished(
            access,
            session_id,
            call,
            &title,
            &result,
            success,
            terminal_call.terminal_id(),
        )?;
        Ok(result)
    }

    async fn handle_command(
        &self,
        runtime: &SessionRuntime,
        connection: &ConnectionTo<Client>,
        session_id: &SessionId,
        prompt: &str,
    ) -> Result<Option<PromptResponse>, Error> {
        let trimmed = prompt.trim();
        if !trimmed.starts_with('/') {
            return Ok(None);
        }
        let (command, argument) = trimmed
            .split_once(char::is_whitespace)
            .map_or((trimmed, ""), |(command, argument)| {
                (command, argument.trim())
            });
        let reply = match command {
            "/help" => {
                "Available commands:\n/plan - enter read-only planning mode\n/agent - enter agent mode\n/model [name] - show or select the model\n/effort [none|low|high|max] - show or select the thinking effort\n/status - show session configuration"
                    .to_string()
            }
            "/plan" => {
                runtime.state.lock().await.record.config.mode = "plan".to_string();
                self.store.save(runtime).await?;
                connection.send_notification(SessionNotification::new(
                    session_id.clone(),
                    agent_client_protocol::schema::v1::SessionUpdate::CurrentModeUpdate(
                        CurrentModeUpdate::new("plan"),
                    ),
                ))?;
                "Plan mode enabled. File mutations and commands are disabled.".to_string()
            }
            "/agent" => {
                runtime.state.lock().await.record.config.mode = "agent".to_string();
                self.store.save(runtime).await?;
                connection.send_notification(SessionNotification::new(
                    session_id.clone(),
                    agent_client_protocol::schema::v1::SessionUpdate::CurrentModeUpdate(
                        CurrentModeUpdate::new("agent"),
                    ),
                ))?;
                "Agent mode enabled.".to_string()
            }
            "/model" if argument.is_empty() => {
                let record = runtime.snapshot().await;
                format!("Current model: {}", record.config.model)
            }
            "/model" => {
                if !self.config.models.iter().any(|model| model == argument) {
                    return Ok(Some(PromptResponse::new(StopReason::Refusal)));
                }
                let mut state = runtime.state.lock().await;
                state.record.config.model = argument.to_string();
                state.record.touch();
                drop(state);
                self.store.save(runtime).await?;
                format!("Model set to {argument}.")
            }
            "/effort" if argument.is_empty() => {
                let record = runtime.snapshot().await;
                format!(
                    "Current thinking effort: {}",
                    record.config.reasoning_effort.as_str()
                )
            }
            "/effort" => match argument.parse::<ReasoningEffort>() {
                Ok(effort) => {
                    let mut state = runtime.state.lock().await;
                    state.record.config.reasoning_effort = effort;
                    state.record.touch();
                    drop(state);
                    self.store.save(runtime).await?;
                    format!("Thinking effort set to {}.", effort.as_str())
                }
                Err(()) => {
                    "Unknown effort. Use none, low, high, or max.".to_string()
                }
            },
            "/status" => {
                let record = runtime.snapshot().await;
                format!(
                    "model: {}\nmode: {}\npermission: {}\neffort: {}\nnetwork: {}\nusage: {} tokens",
                    record.config.model,
                    record.config.mode,
                    record.config.permission_mode.as_str(),
                    record.config.reasoning_effort.as_str(),
                    record.config.network,
                    record.usage.total_tokens
                )
            }
            _ => return Ok(None),
        };
        let message_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
        {
            let mut state = runtime.state.lock().await;
            state
                .record
                .messages
                .push(ChatMessage::assistant(Some(reply.clone()), None));
            state.record.touch();
        }
        self.store.save(runtime).await?;
        send_agent_chunk(connection, session_id, &message_id, reply, false)?;
        connection.send_notification(SessionNotification::new(
            session_id.clone(),
            agent_client_protocol::schema::v1::SessionUpdate::ConfigOptionUpdate(
                ConfigOptionUpdate::new(self.config_options(&runtime.snapshot().await)),
            ),
        ))?;
        Ok(Some(
            // A local command makes no model request.
            PromptResponse::new(StopReason::EndTurn)
                .usage(Some(turn_acp_usage(TokenUsage::default()))),
        ))
    }

    fn config_options(&self, record: &SessionRecord) -> Vec<SessionConfigOption> {
        let model_options = self
            .config
            .models
            .iter()
            .map(|model| SessionConfigSelectOption::new(model.clone(), model.clone()))
            .collect::<Vec<_>>();
        let permission_options = ["ask", "allow", "deny"]
            .into_iter()
            .map(|value| {
                SessionConfigSelectOption::new(
                    value,
                    match value {
                        "ask" => "Ask before sensitive tools",
                        "allow" => "Allow sensitive tools",
                        _ => "Deny sensitive tools",
                    },
                )
            })
            .collect::<Vec<_>>();
        let network_options = vec![
            SessionConfigSelectOption::new("enabled", "Enabled"),
            SessionConfigSelectOption::new("disabled", "Disabled"),
        ];
        vec![
            SessionConfigOption::select(
                "model",
                "Model",
                record.config.model.clone(),
                model_options,
            )
            .description("DeepSeek model used for this session.")
            .category(SessionConfigOptionCategory::Model),
            SessionConfigOption::select(
                "mode",
                "Session mode",
                record.config.mode.clone(),
                vec![
                    SessionConfigSelectOption::new("agent", "Agent"),
                    SessionConfigSelectOption::new("plan", "Plan"),
                ],
            )
            .description("Controls whether mutating tools are available.")
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::select(
                "permission_mode",
                "Tool permissions",
                record.config.permission_mode.as_str(),
                permission_options,
            )
            .description("Permission policy for writes and command execution."),
            SessionConfigOption::select(
                "reasoning_effort",
                "Thinking effort",
                record.config.reasoning_effort.as_str(),
                ReasoningEffort::VALUES
                    .into_iter()
                    .map(|effort| {
                        SessionConfigSelectOption::new(effort.as_str(), effort.label())
                            .description(effort.describe())
                    })
                    .collect::<Vec<_>>(),
            )
            .description("How hard DeepSeek thinks before answering. None disables thinking.")
            .category(SessionConfigOptionCategory::ThoughtLevel),
            SessionConfigOption::select(
                "network",
                "Network access",
                if record.config.network {
                    "enabled"
                } else {
                    "disabled"
                },
                network_options,
            )
            .description("Allow the web_fetch tool to access HTTP(S) URLs."),
        ]
    }

    pub(crate) async fn deepseek_client(&self) -> Result<DeepSeekClient, Error> {
        let routing = self.provider.read().await.clone();
        if !routing.enabled {
            return Err(Error::auth_required()
                .data("the DeepSeek provider is disabled for this ACP connection"));
        }
        DeepSeekClient::with_routing(&self.config, routing.base_url, routing.headers)
    }
}

struct PromptGuard {
    active: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for PromptGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.notify.notify_waiters();
    }
}

fn apply_config_value(
    record: &mut SessionRecord,
    config_id: &str,
    value: &SessionConfigOptionValue,
    models: &[String],
) -> Result<(), Error> {
    match (config_id, value) {
        ("model", SessionConfigOptionValue::ValueId { value })
            if models.iter().any(|model| model == value.0.as_ref()) =>
        {
            record.config.model = value.to_string();
            Ok(())
        }
        ("mode", SessionConfigOptionValue::ValueId { value })
            if matches!(value.to_string().as_str(), "agent" | "plan") =>
        {
            record.config.mode = value.to_string();
            Ok(())
        }
        ("permission_mode", SessionConfigOptionValue::ValueId { value }) => {
            record.config.permission_mode = value
                .to_string()
                .parse()
                .map_err(|()| Error::invalid_params().data("invalid permission mode"))?;
            Ok(())
        }
        ("reasoning_effort", SessionConfigOptionValue::ValueId { value }) => {
            record.config.reasoning_effort = value
                .to_string()
                .parse()
                .map_err(|()| Error::invalid_params().data("invalid reasoning effort"))?;
            Ok(())
        }
        ("network", SessionConfigOptionValue::ValueId { value }) => {
            match value.to_string().as_str() {
                "enabled" => record.config.network = true,
                "disabled" => record.config.network = false,
                _ => {
                    return Err(Error::invalid_params().data("network must be enabled or disabled"));
                }
            }
            Ok(())
        }
        _ => {
            Err(Error::invalid_params()
                .data(format!("unknown or invalid config option: {config_id}")))
        }
    }
}

fn mode_state(record: &SessionRecord) -> SessionModeState {
    SessionModeState::new(
        SessionModeId::new(record.config.mode.clone()),
        vec![
            SessionMode::new("agent", "Agent")
                .description("Inspect, edit, test, and complete the requested task."),
            SessionMode::new("plan", "Plan")
                .description("Read-only analysis and planning; mutating tools are disabled."),
        ],
    )
}

fn session_info(record: SessionRecord) -> SessionInfo {
    let title = session_title(&record);
    SessionInfo::new(record.id, record.cwd)
        .additional_directories(record.additional_directories)
        .title(title)
        .updated_at(record.updated_at.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn send_agent_chunk(
    connection: &ConnectionTo<Client>,
    session_id: &SessionId,
    message_id: &str,
    text: String,
    thought: bool,
) -> Result<(), Error> {
    if text.is_empty() {
        return Ok(());
    }
    let chunk =
        ContentChunk::new(ContentBlock::Text(TextContent::new(text))).message_id(message_id);
    let update = if thought {
        agent_client_protocol::schema::v1::SessionUpdate::AgentThoughtChunk(chunk)
    } else {
        agent_client_protocol::schema::v1::SessionUpdate::AgentMessageChunk(chunk)
    };
    connection.send_notification(SessionNotification::new(session_id.clone(), update))
}

fn send_usage_update(
    connection: &ConnectionTo<Client>,
    record: &SessionRecord,
) -> Result<(), Error> {
    connection.send_notification(SessionNotification::new(
        record.id.clone(),
        agent_client_protocol::schema::v1::SessionUpdate::UsageUpdate(UsageUpdate::new(
            record.context_used,
            DEFAULT_CONTEXT_WINDOW,
        )),
    ))
}

fn send_available_commands(
    connection: &ConnectionTo<Client>,
    session_id: &str,
) -> Result<(), Error> {
    connection.send_notification(SessionNotification::new(
        SessionId::new(session_id.to_string()),
        agent_client_protocol::schema::v1::SessionUpdate::AvailableCommandsUpdate(
            AvailableCommandsUpdate::new(vec![
                AvailableCommand::new("help", "Show available harness commands"),
                AvailableCommand::new("plan", "Enter read-only planning mode"),
                AvailableCommand::new("agent", "Enter agent mode"),
                AvailableCommand::new("model", "Show or select the model"),
                AvailableCommand::new("status", "Show session configuration and usage"),
            ]),
        ),
    ))
}

/// Send a tool call's final update. `terminal` is the client terminal a
/// command ran in. The update keeps it, so a client that keeps showing a
/// released terminal's output still shows it. The update adds the result text
/// only if the tool failed, because after a completed command that text
/// repeats the terminal's output; `rawOutput` still carries the exit status
/// and output. Replay after `session/load` shows the result text instead,
/// because a released terminal cannot be shown again.
fn send_tool_finished(
    access: &ToolAccess,
    session_id: &SessionId,
    call: &crate::model::ToolCall,
    title: &str,
    result: &ToolResult,
    success: bool,
    terminal: Option<&TerminalId>,
) -> Result<(), Error> {
    let mut content = Vec::new();
    if let Some(terminal_id) = terminal {
        content.push(ToolCallContent::Terminal(Terminal::new(
            terminal_id.clone(),
        )));
    }
    if terminal.is_none() || !success {
        let text = truncate_text(&result.text, access.config.tool_output_bytes);
        content.push(ToolCallContent::Content(
            agent_client_protocol::schema::v1::Content::new(ContentBlock::Text(TextContent::new(
                text,
            ))),
        ));
    }
    for item in structured_content(&result.raw) {
        if let Ok(block) = serde_json::from_value::<ContentBlock>(item) {
            content.push(ToolCallContent::Content(
                agent_client_protocol::schema::v1::Content::new(block),
            ));
        }
    }
    if let Some(diff) = crate::tools::persisted_diff(&call.function.name, &result.raw) {
        content.push(diff_content(&diff));
    }
    let fields = ToolCallUpdateFields::new()
        .title(Some(title.to_string()))
        .status(Some(if success {
            ToolCallStatus::Completed
        } else {
            ToolCallStatus::Failed
        }))
        .content(Some(content))
        .raw_output(Some(result.raw.clone()));
    access
        .connection
        .as_ref()
        .expect("v1 tool result requires an ACP v1 connection")
        .send_notification(SessionNotification::new(
            session_id.clone(),
            agent_client_protocol::schema::v1::SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                call.id.clone(),
                fields,
            )),
        ))
}

/// A file change as ACP v1 tool call content: the whole file before and after.
fn diff_content(diff: &PersistedDiff) -> ToolCallContent {
    ToolCallContent::Diff(
        agent_client_protocol::schema::v1::Diff::new(diff.path.clone(), diff.new_text.clone())
            .old_text(diff.old_text.clone()),
    )
}

enum PermissionDecision {
    AllowOnce,
    AllowAlways,
    Deny,
    Cancelled,
}

async fn ask_user(
    access: &ToolAccess,
    store: &SessionStore,
    session_id: &SessionId,
    call: &crate::model::ToolCall,
    message: &str,
    arguments: &Value,
    cancellation: &CancellationToken,
) -> Result<ToolResult, Error> {
    let scope = ElicitationSessionScope::new(session_id.clone())
        .tool_call_id(Some(ToolCallId::new(call.id.clone())));
    let mut url_elicitation_id = None;
    let mode = if let Some(url) = arguments.get("url").and_then(Value::as_str) {
        if !access
            .client
            .elicitation
            .as_ref()
            .is_some_and(|capabilities| capabilities.supports_url())
        {
            return Err(
                Error::invalid_request().data("the Client does not support URL elicitation")
            );
        }
        let parsed = reqwest::Url::parse(url)
            .map_err(|error| Error::invalid_params().data(error.to_string()))?;
        if parsed.username() != "" || parsed.password().is_some() {
            return Err(Error::invalid_params()
                .data("URL elicitations must not contain embedded credentials"));
        }
        if parsed.scheme() != "https" && parsed.host_str() != Some("localhost") {
            return Err(
                Error::invalid_params().data("URL elicitations require HTTPS outside localhost")
            );
        }
        let elicitation_id = arguments
            .get("elicitation_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("elicit-{}", uuid::Uuid::new_v4().simple()));
        if !store
            .reserve_url_elicitation(&session_id.to_string(), &elicitation_id)
            .await
        {
            return Err(Error::invalid_params().data(format!(
                "elicitation ID {elicitation_id} is still in use by a URL elicitation that was not reported complete; omit elicitation_id to generate a new one"
            )));
        }
        url_elicitation_id = Some(elicitation_id.clone());
        ElicitationMode::Url(ElicitationUrlMode::new(scope, elicitation_id, url))
    } else {
        if !access
            .client
            .elicitation
            .as_ref()
            .is_some_and(|capabilities| capabilities.supports_form())
        {
            return Err(
                Error::invalid_request().data("the Client does not support form elicitation")
            );
        }
        let schema = ElicitationSchema::new()
            .title("User input")
            .description("Provide the information requested by the agent.")
            .string("response", true);
        ElicitationMode::Form(ElicitationFormMode::new(scope, schema))
    };
    let request = CreateElicitationRequest::new(mode, message);
    let response: Result<CreateElicitationResponse, Error> = tokio::select! {
        // The Client may already have accepted, so a URL elicitation's ID
        // stays reserved, though never completable, until the session closes.
        _ = cancellation.cancelled() => return Err(Error::request_cancelled()),
        response = access
            .connection
            .as_ref()
            .expect("v1 elicitation requires an ACP v1 connection")
            .send_request(request)
            .block_task() => response,
    };
    if let Some(elicitation_id) = &url_elicitation_id {
        let accepted = response
            .as_ref()
            .is_ok_and(|response| matches!(response.action, ElicitationAction::Accept(_)));
        store
            .settle_url_elicitation(&session_id.to_string(), elicitation_id, accepted)
            .await;
    }
    match response?.action {
        ElicitationAction::Accept(action) => {
            // Consent, not completion. What the user enters out of band must
            // stay out of the model's context, so URL content is not read.
            if let Some(elicitation_id) = &url_elicitation_id {
                return Ok(crate::tools::url_elicitation_accepted(elicitation_id));
            }
            let response = action
                .content
                .as_ref()
                .and_then(|content| content.get("response"))
                .map(elicitation_value_text);
            match response {
                Some(response) => Ok(ToolResult {
                    text: response.clone(),
                    raw: json!({"response": response, "action": "accept"}),
                }),
                None => Ok(ToolResult {
                    text: "The user accepted without providing a response.".to_string(),
                    raw: json!({"action": "accept"}),
                }),
            }
        }
        ElicitationAction::Decline => Ok(ToolResult {
            text: "The user declined to answer.".to_string(),
            raw: json!({"action": "decline"}),
        }),
        ElicitationAction::Cancel => Ok(ToolResult {
            text: "The user cancelled the question.".to_string(),
            raw: json!({"action": "cancel"}),
        }),
        _ => Ok(ToolResult {
            text: "The client returned an unsupported elicitation action.".to_string(),
            raw: json!({"action": "unknown"}),
        }),
    }
}

fn elicitation_value_text(value: &ElicitationContentValue) -> String {
    match value {
        ElicitationContentValue::String(value) => value.clone(),
        ElicitationContentValue::Integer(value) => value.to_string(),
        ElicitationContentValue::Number(value) => value.to_string(),
        ElicitationContentValue::Boolean(value) => value.to_string(),
        ElicitationContentValue::StringArray(values) => values.join(", "),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "<unsupported value>".to_string()),
    }
}

async fn request_permission(
    access: &ToolAccess,
    session_id: &SessionId,
    call: &crate::model::ToolCall,
    title: &str,
    arguments: &Value,
    preview: Option<&PersistedDiff>,
    cancellation: &CancellationToken,
) -> Result<PermissionDecision, Error> {
    let options = vec![
        PermissionOption::new(
            PermissionOptionId::new("allow_once"),
            "Allow once",
            PermissionOptionKind::AllowOnce,
        ),
        PermissionOption::new(
            PermissionOptionId::new("allow_always"),
            crate::tools::session_approval_label(&call.function.name),
            PermissionOptionKind::AllowAlways,
        ),
        PermissionOption::new(
            PermissionOptionId::new("reject_once"),
            "Deny",
            PermissionOptionKind::RejectOnce,
        ),
    ];
    let fields = ToolCallUpdateFields::new()
        .title(Some(title.to_string()))
        .kind(Some(tool_kind(&call.function.name)))
        .status(Some(ToolCallStatus::Pending))
        .content(preview.map(|diff| vec![diff_content(diff)]))
        .raw_input(Some(arguments.clone()));
    let request = RequestPermissionRequest::new(
        session_id.clone(),
        ToolCallUpdate::new(call.id.clone(), fields),
        options,
    );
    let response = tokio::select! {
        _ = cancellation.cancelled() => return Ok(PermissionDecision::Cancelled),
        response = access
            .connection
            .as_ref()
            .expect("v1 permissions require an ACP v1 connection")
            .send_request(request)
            .block_task() => response?,
    };
    Ok(match response.outcome {
        RequestPermissionOutcome::Cancelled => PermissionDecision::Cancelled,
        RequestPermissionOutcome::Selected(SelectedPermissionOutcome { option_id, .. }) => {
            match option_id.to_string().as_str() {
                "allow_once" => PermissionDecision::AllowOnce,
                "allow_always" => PermissionDecision::AllowAlways,
                _ => PermissionDecision::Deny,
            }
        }
        _ => PermissionDecision::Deny,
    })
}

fn tool_kind(name: &str) -> ToolKind {
    match name {
        "read_file" => ToolKind::Read,
        "write_file" | "edit_file" => ToolKind::Edit,
        "search_files" | "list_directory" => ToolKind::Search,
        "run_command" => ToolKind::Execute,
        "web_fetch" => ToolKind::Fetch,
        name if name.starts_with("mcp__") => ToolKind::Other,
        _ => ToolKind::Other,
    }
}

fn parse_plan(arguments: &Value) -> Result<Plan, Error> {
    let entries = arguments
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid_params().data("update_plan requires an entries array"))?
        .iter()
        .map(|entry| {
            let content = entry
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::invalid_params().data("plan entry requires content"))?;
            let priority = match entry.get("priority").and_then(Value::as_str) {
                Some("high") => PlanEntryPriority::High,
                Some("medium") | None => PlanEntryPriority::Medium,
                Some("low") => PlanEntryPriority::Low,
                Some(value) => {
                    return Err(
                        Error::invalid_params().data(format!("invalid plan priority: {value}"))
                    );
                }
            };
            let status = match entry.get("status").and_then(Value::as_str) {
                Some("pending") | None => PlanEntryStatus::Pending,
                Some("in_progress") => PlanEntryStatus::InProgress,
                Some("completed") => PlanEntryStatus::Completed,
                Some(value) => {
                    return Err(
                        Error::invalid_params().data(format!("invalid plan status: {value}"))
                    );
                }
            };
            Ok(PlanEntry::new(content, priority, status))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    if entries.len() > 50 {
        return Err(Error::invalid_params().data("a plan may contain at most 50 entries"));
    }
    Ok(Plan::new(entries))
}

fn tool_title(name: &str, arguments: &Value) -> String {
    match name {
        "run_command" => format!("Run {}", crate::tools::command_display(arguments)),
        "web_fetch" => format!(
            "Fetch {}",
            arguments
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or("URL")
        ),
        _ => arguments.get("path").and_then(Value::as_str).map_or_else(
            || name.replace('_', " "),
            |path| format!("{} {path}", name.replace('_', " ")),
        ),
    }
}

fn tool_locations(name: &str, arguments: &Value, session: &SessionRecord) -> Vec<ToolCallLocation> {
    if !matches!(name, "read_file" | "write_file" | "edit_file") {
        return Vec::new();
    }
    let Some(path) = arguments.get("path").and_then(Value::as_str) else {
        return Vec::new();
    };
    let path = {
        let path = std::path::Path::new(path);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            session.cwd.join(path)
        }
    };
    let mut location = ToolCallLocation::new(path);
    if name == "read_file"
        && let Some(line) = arguments
            .get("start_line")
            .and_then(Value::as_u64)
            .and_then(|line| u32::try_from(line).ok())
    {
        location = location.line(Some(line));
    }
    vec![location]
}

struct PromptPayload {
    text: String,
    content: MessageContent,
    has_images: bool,
}

fn content_blocks_to_prompt(blocks: &[ContentBlock]) -> Result<PromptPayload, Error> {
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
                EmbeddedResourceResource::TextResourceContents(text) => {
                    let text = format!("[embedded resource: {}]\n{}", text.uri, text.text);
                    text_parts.push(text.clone());
                    content_parts.push(ContentPart::Text { text });
                }
                EmbeddedResourceResource::BlobResourceContents(blob) => {
                    let mime = blob
                        .mime_type
                        .as_deref()
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
                    let text = "[unsupported embedded resource]".to_string();
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
                let text = "[unsupported content block]".to_string();
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
    Ok(PromptPayload {
        text,
        content,
        has_images,
    })
}

fn message_content_to_v1_blocks(content: &MessageContent) -> Vec<ContentBlock> {
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
                    Some(ContentBlock::Image(
                        agent_client_protocol::schema::v1::ImageContent::new(data, mime_type),
                    ))
                }
                ContentPart::File { file_data, .. } => {
                    let data_url = file_data.as_deref()?.strip_prefix("data:")?;
                    let (mime_type, data) = data_url.split_once(";base64,")?;
                    Some(ContentBlock::Image(
                        agent_client_protocol::schema::v1::ImageContent::new(data, mime_type),
                    ))
                }
            })
            .collect(),
    }
}

fn client_system_prompt(
    prompt: Option<&agent_client_protocol::schema::v1::SystemPrompt>,
) -> Result<Option<ClientSystemPrompt>, Error> {
    let Some(prompt) = prompt else {
        return Ok(None);
    };
    let mode = match &prompt.mode {
        SystemPromptMode::Append => ClientPromptMode::Append,
        SystemPromptMode::Override => ClientPromptMode::Override,
        mode => {
            return Err(Error::invalid_params().data(format!(
                "unsupported client system prompt mode {}; this agent supports append and override",
                serde_json::to_string(mode).unwrap_or_default()
            )));
        }
    };
    let text =
        prompt
            .content
            .iter()
            .map(|block| match block {
                ContentBlock::Text(text) => Ok(text.text.as_str()),
                _ => Err(Error::invalid_params()
                    .data("client system prompts may contain only text blocks")),
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("\n\n");
    Ok(Some(ClientSystemPrompt { mode, text }))
}

fn replay_history(
    connection: &ConnectionTo<Client>,
    record: &SessionRecord,
    replay_compactions: bool,
) -> Result<(), Error> {
    let session_id = SessionId::new(record.id.clone());
    let mut pending_calls = Vec::<String>::new();
    for (index, message) in record.messages.iter().enumerate() {
        let message_id = record
            .message_ids
            .get(&index.to_string())
            .cloned()
            .unwrap_or_else(|| format!("history-message-{}", uuid::Uuid::new_v4().simple()));
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
                        .unwrap_or_else(|| message_content_to_v1_blocks(content));
                    for block in blocks {
                        connection.send_notification(SessionNotification::new(
                            session_id.clone(),
                            agent_client_protocol::schema::v1::SessionUpdate::UserMessageChunk(
                                ContentChunk::new(block).message_id(
                                    agent_client_protocol::schema::v1::MessageId::new(
                                        message_id.clone(),
                                    ),
                                ),
                            ),
                        ))?;
                    }
                }
            }
            Role::Assistant => {
                if let Some(reasoning) = &message.reasoning_content {
                    let thought_id = format!("history-thought-{}", uuid::Uuid::new_v4().simple());
                    send_replay_thought(connection, &session_id, &thought_id, reasoning)?;
                }
                if let Some(content) = &message.content {
                    for block in message_content_to_v1_blocks(content) {
                        connection.send_notification(SessionNotification::new(
                            session_id.clone(),
                            agent_client_protocol::schema::v1::SessionUpdate::AgentMessageChunk(
                                ContentChunk::new(block).message_id(
                                    agent_client_protocol::schema::v1::MessageId::new(
                                        message_id.clone(),
                                    ),
                                ),
                            ),
                        ))?;
                    }
                }
                for call in &message.tool_calls {
                    let input = serde_json::from_str(&call.function.arguments)
                        .unwrap_or_else(|_| Value::String(call.function.arguments.clone()));
                    connection.send_notification(SessionNotification::new(
                        session_id.clone(),
                        agent_client_protocol::schema::v1::SessionUpdate::ToolCall(
                            ToolCall::new(call.id.clone(), call.function.name.clone())
                                .name(call.function.name.clone())
                                .status(ToolCallStatus::Pending)
                                .raw_input(input),
                        ),
                    ))?;
                    if call.function.name == "update_plan"
                        && let Ok(arguments) =
                            serde_json::from_str::<Value>(&call.function.arguments)
                        && let Ok(plan) = parse_plan(&arguments)
                    {
                        connection.send_notification(SessionNotification::new(
                            session_id.clone(),
                            agent_client_protocol::schema::v1::SessionUpdate::Plan(plan),
                        ))?;
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
                            ToolOutcomeStatus::Cancelled => ToolCallStatus::Failed,
                        });
                    let mut content = vec![ToolCallContent::Content(
                        agent_client_protocol::schema::v1::Content::new(ContentBlock::Text(
                            TextContent::new(
                                message
                                    .content
                                    .as_ref()
                                    .map(crate::model::MessageContent::text_value)
                                    .unwrap_or_default(),
                            ),
                        )),
                    )];
                    if let Some(outcome) = outcome {
                        for item in &outcome.structured_content {
                            if let Ok(block) = serde_json::from_value::<ContentBlock>(item.clone())
                            {
                                content.push(ToolCallContent::Content(
                                    agent_client_protocol::schema::v1::Content::new(block),
                                ));
                            }
                        }
                        if let Some(diff) = &outcome.diff {
                            content.push(diff_content(diff));
                        }
                    }
                    let fields = ToolCallUpdateFields::new()
                        .status(Some(status))
                        .content(Some(content))
                        .raw_output(outcome.map(|outcome| outcome.raw_output.clone()));
                    connection.send_notification(SessionNotification::new(
                        session_id.clone(),
                        agent_client_protocol::schema::v1::SessionUpdate::ToolCallUpdate(
                            ToolCallUpdate::new(tool_call_id.clone(), fields),
                        ),
                    ))?;
                    pending_calls.retain(|id| id != tool_call_id);
                }
            }
            Role::System => {}
        }
    }
    for tool_call_id in pending_calls {
        connection.send_notification(SessionNotification::new(
            session_id.clone(),
            agent_client_protocol::schema::v1::SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                tool_call_id,
                ToolCallUpdateFields::new().status(ToolCallStatus::Failed),
            )),
        ))?;
    }
    for pending in &record.pending_steering {
        let blocks =
            serde_json::from_value::<Vec<ContentBlock>>(Value::Array(pending.blocks.clone()))
                .unwrap_or_else(|_| message_content_to_v1_blocks(&pending.content));
        for block in blocks {
            connection.send_notification(SessionNotification::new(
                session_id.clone(),
                agent_client_protocol::schema::v1::SessionUpdate::UserMessageChunk(
                    ContentChunk::new(block).message_id(
                        agent_client_protocol::schema::v1::MessageId::new(
                            pending.message_id.clone(),
                        ),
                    ),
                ),
            ))?;
        }
    }
    connection.send_notification(SessionNotification::new(
        session_id.clone(),
        agent_client_protocol::schema::v1::SessionUpdate::SessionInfoUpdate(
            SessionInfoUpdate::new().title(record.title.clone()),
        ),
    ))?;
    if replay_compactions {
        for compaction in &record.compactions {
            let status = match compaction.status.as_str() {
                "failed" => CompactionStatus::Failed,
                "cancelled" => CompactionStatus::Cancelled,
                "in_progress" => CompactionStatus::InProgress,
                _ => CompactionStatus::Completed,
            };
            connection.send_notification(SessionNotification::new(
                session_id.clone(),
                agent_client_protocol::schema::v1::SessionUpdate::CompactionUpdate(
                    CompactionUpdate::new(compaction.id.clone(), status),
                ),
            ))?;
        }
    }
    send_usage_update(connection, record)
}

fn send_replay_thought(
    connection: &ConnectionTo<Client>,
    session_id: &SessionId,
    message_id: &str,
    text: &str,
) -> Result<(), Error> {
    let content =
        ContentChunk::new(ContentBlock::Text(TextContent::new(text))).message_id(message_id);
    connection.send_notification(SessionNotification::new(
        session_id.clone(),
        agent_client_protocol::schema::v1::SessionUpdate::AgentThoughtChunk(content),
    ))
}

/// A whole turn's usage, declared as such so a client that cannot otherwise
/// tell a turn's usage from a session total (Mjolnir) counts it.
fn turn_acp_usage(usage: TokenUsage) -> Usage {
    acp_usage(usage).meta(Some(serde_json::Map::from_iter([(
        "mjolnir.dev/usage-scope".to_string(),
        json!("turn"),
    )])))
}

fn acp_usage(usage: TokenUsage) -> Usage {
    Usage::new(
        usage.total_tokens,
        usage.prompt_tokens,
        usage.completion_tokens,
    )
    .cached_read_tokens(Some(usage.prompt_cache_hit_tokens))
}

fn truncate_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}\n[...truncated...]", &value[..boundary])
}
