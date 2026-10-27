use clap::Parser;
use dsh_acp::{Config, run};

#[derive(Debug, Parser)]
#[command(
    name = "dsh-acp",
    about = "DeepSeek-powered Agent Client Protocol server",
    version
)]
struct Cli {
    /// DeepSeek API key. Defaults to DEEPSEEK_API_KEY.
    #[arg(long, env = "DEEPSEEK_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// OpenAI-compatible DeepSeek API base URL.
    #[arg(
        long,
        env = "DEEPSEEK_BASE_URL",
        default_value = "https://api.deepseek.com"
    )]
    base_url: String,

    /// Default model.
    #[arg(long, env = "DEEPSEEK_MODEL", default_value = "deepseek-flash")]
    model: String,

    /// Comma-separated models exposed by the model selector.
    #[arg(
        long,
        env = "DSH_ACP_MODELS",
        default_value = "deepseek-flash,deepseek-v4-pro"
    )]
    models: String,

    /// Session storage directory.
    #[arg(long, env = "DSH_ACP_DATA_DIR", default_value = ".dsh-acp")]
    data_dir: std::path::PathBuf,

    /// Permission policy: ask, allow, or deny.
    #[arg(long, env = "DSH_ACP_PERMISSION_MODE", default_value = "ask")]
    permission_mode: String,

    /// Extra system instructions added after the built-in harness prompt, or
    /// after a client prompt that overrides it.
    #[arg(long, env = "DSH_ACP_SYSTEM_PROMPT")]
    system_prompt: Option<String>,

    /// Maximum response tokens per model completion.
    ///
    /// Omitted by default, so DeepSeek uses its maximum (384000 for both
    /// models). Thinking tokens count toward this cap. Set it only to bound
    /// cost.
    #[arg(long, env = "DSH_ACP_MAX_TOKENS")]
    max_tokens: Option<u32>,

    /// Thinking effort: none, low, high, or max. "none" disables thinking mode.
    #[arg(
        long,
        env = "DSH_ACP_REASONING_EFFORT",
        default_value = "high",
        value_name = "EFFORT"
    )]
    reasoning_effort: String,

    /// Approximate character budget for conversation context. DeepSeek accepts
    /// 1M tokens (~3.3M characters) of context.
    #[arg(long, env = "DSH_ACP_CONTEXT_CHARS", default_value_t = 2_000_000)]
    context_chars: usize,

    /// Maximum bytes retained from a single tool result.
    #[arg(long, env = "DSH_ACP_TOOL_OUTPUT_BYTES", default_value_t = 131_072)]
    tool_output_bytes: usize,

    /// Seconds to wait for a connection, or for a stalled stream to resume.
    #[arg(long, env = "DSH_ACP_HTTP_TIMEOUT_SECS", default_value_t = 180)]
    http_timeout_secs: u64,

    /// Disable web_fetch even when tools are available.
    #[arg(long, env = "DSH_ACP_DISABLE_WEB", default_value_t = false)]
    disable_web: bool,

    /// Include protocol traffic diagnostics on stderr (never writes stdout).
    #[arg(long, env = "DSH_ACP_TRACE", default_value_t = false)]
    trace: bool,
}

impl TryFrom<Cli> for Config {
    type Error = agent_client_protocol::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let mut models = cli
            .models
            .split(',')
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if models.is_empty() {
            models.push("deepseek-flash".to_string());
        }
        if !models.iter().any(|model| model == &cli.model) {
            models.insert(0, cli.model.clone());
        }
        let mut seen = std::collections::HashSet::new();
        models.retain(|model| seen.insert(model.clone()));
        let permission_mode = cli.permission_mode.parse().map_err(|()| {
            agent_client_protocol::Error::invalid_params()
                .data("permission mode must be ask, allow, or deny")
        })?;
        let reasoning_effort = cli.reasoning_effort.parse().map_err(|()| {
            agent_client_protocol::Error::invalid_params()
                .data("reasoning effort must be none, low, high, or max")
        })?;
        Ok(Self {
            api_key: cli.api_key,
            base_url: cli.base_url,
            default_model: cli.model,
            models,
            data_dir: cli.data_dir,
            permission_mode,
            system_prompt: cli.system_prompt,
            max_tokens: cli.max_tokens,
            reasoning_effort,
            context_chars: cli.context_chars,
            tool_output_bytes: cli.tool_output_bytes,
            http_timeout: std::time::Duration::from_secs(cli.http_timeout_secs),
            disable_web: cli.disable_web,
            trace: cli.trace,
        })
    }
}

#[tokio::main]
async fn main() -> agent_client_protocol::Result<()> {
    let cli = Cli::parse();
    let config = Config::try_from(cli)?;
    config.validate()?;
    run(config).await
}
