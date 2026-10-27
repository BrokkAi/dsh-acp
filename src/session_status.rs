//! `session/status`, from the draft
//! [Session Status RFD](https://agentclientprotocol.com/rfds/session-status).
//!
//! agent-client-protocol 3.4 has no types for this method, so its request,
//! error, and capability marker are hand-rolled here until the SDK has them.
//! Each protocol version builds its own response, because only v2 reports
//! foreground state.

use agent_client_protocol::Error;
use serde_json::{Value, json};

pub(crate) const METHOD: &str = "session/status";

/// The session a `session/status` request asks about.
pub(crate) fn requested_session_id(params: &Value) -> Result<&str, Error> {
    params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid_params().data("session/status requires a string sessionId"))
}

/// The error for a session the agent does not know.
pub(crate) fn unknown_session(session_id: &str) -> Error {
    Error::resource_not_found(None).data(json!({ "sessionId": session_id }))
}

/// Serializes an initialize response and adds the `status` marker to the
/// session capabilities at `pointer`. The SDK's capability types have no
/// field for it.
pub(crate) fn advertise(response: impl serde::Serialize, pointer: &str) -> Result<Value, Error> {
    let mut response = serde_json::to_value(response).map_err(Error::into_internal_error)?;
    response
        .pointer_mut(pointer)
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            Error::internal_error().data("initialize response has no session capabilities")
        })?
        .insert("status".to_string(), json!({}));
    Ok(response)
}
