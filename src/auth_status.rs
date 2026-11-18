//! `auth/status`, from the draft
//! [Agent Authentication State Query RFD](https://agentclientprotocol.com/rfds/get-auth-state).
//!
//! Both protocol versions report the same state, each in its own SDK types.

use crate::agent::{Credential, Harness};

pub(crate) const METHOD: &str = "auth/status";

/// The local authentication state `auth/status` reports.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AuthStatus {
    /// Whether DeepSeek requests on this connection would carry a credential.
    pub(crate) authenticated: bool,
    /// The configured credential, if any, as display text. It never contains
    /// the credential itself.
    pub(crate) account: Option<AuthAccountInfo>,
}

/// Display text for the configured credential.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AuthAccountInfo {
    pub(crate) label: &'static str,
    /// Where the credential comes from.
    pub(crate) description: &'static str,
}

impl Harness {
    /// The `auth/status` state. It reads local state only and changes
    /// nothing.
    ///
    /// `authenticated` means DeepSeek requests on this connection would carry
    /// a credential, not that DeepSeek accepts it. After a logout or a
    /// disabled provider, prompts fail with `auth_required` until the client
    /// authenticates again, so that state reads as unauthenticated even though
    /// the key is still configured; its account is then reported as inactive.
    pub(crate) async fn auth_status(&self) -> AuthStatus {
        let routing = self.provider.read().await;
        let account = routing
            .credential(&self.config)
            .map(|credential| match credential {
                Credential::ProviderHeader => AuthAccountInfo {
                    label: "DeepSeek provider header",
                    description: "Authorization header from the provider configuration",
                },
                Credential::ApiKey => AuthAccountInfo {
                    label: "DeepSeek API key",
                    description: "DEEPSEEK_API_KEY or --api-key-file",
                },
            });
        AuthStatus {
            authenticated: account.is_some() && routing.enabled,
            account,
        }
    }
}

/// Builds one protocol version's `auth/status` response, or its
/// `auth/status_update` notification, from the shared state. The two carry
/// the same fields, and the SDK's v1 and v2 types have the same shape but are
/// distinct.
macro_rules! auth_status_message {
    ($schema:path, $message:ident, $status:expr) => {{
        use $schema as schema;
        let status: $crate::auth_status::AuthStatus = $status;
        schema::$message::new(if status.authenticated {
            schema::AuthState::Authenticated
        } else {
            schema::AuthState::Unauthenticated
        })
        .accounts(
            status
                .account
                .into_iter()
                .map(|account| {
                    schema::AuthAccount::new(account.label)
                        .description(account.description.to_string())
                        .active(status.authenticated)
                })
                .collect(),
        )
    }};
}
pub(crate) use auth_status_message;
