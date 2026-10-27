//! `auth/status`, from the draft
//! [Agent Authentication State Query RFD](https://agentclientprotocol.com/rfds/get-auth-state).
//!
//! agent-client-protocol 3.4 has no types for this method, so its response is
//! hand-rolled here until the SDK has them. Both protocol versions report the
//! same state. Each version's `initialize` adds the `auth.status` capability
//! with `agent::with_extra_capabilities`.

use serde_json::{Value, json};

use crate::agent::{Credential, Harness};

pub(crate) const METHOD: &str = "auth/status";

impl Harness {
    /// The `auth/status` result. It reads local state only and changes
    /// nothing.
    ///
    /// `authenticated` means DeepSeek requests on this connection would carry
    /// a credential, not that DeepSeek accepts it. After a logout or a
    /// disabled provider, prompts fail with `auth_required` until the client
    /// authenticates again, so that state reads as unauthenticated even though
    /// the key is still configured. The result never contains the credential.
    pub(crate) async fn auth_status(&self) -> Value {
        let routing = self.provider.read().await;
        let (authenticated, message) = match (routing.credential(&self.config), routing.enabled) {
            (None, _) => (
                false,
                "No DeepSeek credentials are configured. Set DEEPSEEK_API_KEY, pass --api-key, \
                 or configure an Authorization provider header.",
            ),
            (Some(_), false) => (
                false,
                "The DeepSeek provider is disabled for this connection. Authenticate or \
                 configure the provider to enable it.",
            ),
            (Some(Credential::ProviderHeader), true) => (
                true,
                "Using the Authorization header from the provider configuration.",
            ),
            (Some(Credential::ApiKey), true) => (
                true,
                "Using the DeepSeek API key from DEEPSEEK_API_KEY or --api-key.",
            ),
        };
        json!({"authenticated": authenticated, "message": message})
    }
}
