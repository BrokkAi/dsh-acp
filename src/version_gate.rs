//! Spec version negotiation for requests below every supported version.
//!
//! The ACP v2 initialization spec requires an Agent that implements v2 to
//! select the highest version it supports at or below the requested one, and
//! to reject `initialize` with `-32022` when there is none. The SDK's protocol
//! router instead hands a request below v1 to the v1 implementation and
//! answers it with v1, so this gate answers such a request itself before the
//! router sees it.

use std::sync::{Arc, Mutex, PoisonError};

use agent_client_protocol::{
    Agent, Channel, Client, ConnectTo, Error, RawJsonRpcError, RawJsonRpcId, RawJsonRpcMessage,
    RawJsonRpcParams, RawJsonRpcResponse, TransportFrame, schema::ProtocolVersion,
    v2::schema as v2,
};
use futures_util::StreamExt;

/// The versions the agent's protocol router serves.
const SUPPORTED: [ProtocolVersion; 2] = [ProtocolVersion::V1, ProtocolVersion::V2];

/// Wraps an agent so `initialize` requests below v1 get `-32022` until the
/// connection is initialized. Every other frame passes through unchanged.
pub(crate) struct VersionGate<C>(pub(crate) C);

/// What the gate has seen of the `initialize` exchange.
#[derive(Default)]
struct Negotiation {
    /// IDs of `initialize` requests forwarded to the agent and not yet
    /// answered.
    pending: Vec<RawJsonRpcId>,
    /// Set once the agent answers an `initialize` successfully. Repeats after
    /// that are negotiated by the selected implementation alone.
    initialized: bool,
}

impl<C: ConnectTo<Client>> ConnectTo<Client> for VersionGate<C> {
    async fn connect_to(self, client: impl ConnectTo<Agent>) -> Result<(), Error> {
        let (gate_agent, agent_end) = Channel::duplex();
        let (gate_client, client_end) = Channel::duplex();
        let negotiation = Arc::new(Mutex::new(Negotiation::default()));

        let Channel {
            rx: mut from_client,
            tx: to_client,
            ..
        } = gate_client;
        let Channel {
            rx: mut from_agent,
            tx: to_agent,
            ..
        } = gate_agent;

        let inbound = {
            let negotiation = negotiation.clone();
            let to_client = to_client.clone();
            async move {
                while let Some(frame) = from_client.next().await {
                    let mut state = negotiation.lock().unwrap_or_else(PoisonError::into_inner);
                    if !state.initialized
                        && let Some((id, requested)) = initialize_request(&frame)
                    {
                        if requested < ProtocolVersion::V1 {
                            drop(state);
                            let _ = to_client.unbounded_send(unsupported(id, requested));
                            continue;
                        }
                        state.pending.push(id);
                    }
                    drop(state);
                    if to_agent.unbounded_send(frame).is_err() {
                        break;
                    }
                }
                Ok::<(), Error>(())
            }
        };
        let outbound = async move {
            while let Some(frame) = from_agent.next().await {
                if let TransportFrame::Single(RawJsonRpcMessage::Response(response)) = &frame {
                    let mut state = negotiation.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(index) = state.pending.iter().position(|id| id == response.id()) {
                        state.pending.swap_remove(index);
                        if matches!(response, RawJsonRpcResponse::Result { .. }) {
                            state.initialized = true;
                            state.pending.clear();
                        }
                    }
                }
                if to_client.unbounded_send(frame).is_err() {
                    break;
                }
            }
            Ok::<(), Error>(())
        };

        futures_util::try_join!(
            self.0.connect_to(agent_end),
            ConnectTo::<Client>::connect_to(client_end, client),
            inbound,
            outbound,
        )?;
        Ok(())
    }
}

/// The ID and requested version of an `initialize` request whose
/// `protocolVersion` is a valid version. Malformed versions are left to the
/// router, which rejects them with `-32602`.
fn initialize_request(frame: &TransportFrame) -> Option<(RawJsonRpcId, ProtocolVersion)> {
    let TransportFrame::Single(RawJsonRpcMessage::Request(request)) = frame else {
        return None;
    };
    if request.method.as_ref() != "initialize" {
        return None;
    }
    let Some(RawJsonRpcParams::Object(params)) = &request.params else {
        return None;
    };
    let requested = serde_json::from_value(params.get("protocolVersion")?.clone()).ok()?;
    Some((request.id.clone(), requested))
}

fn unsupported(id: RawJsonRpcId, requested: ProtocolVersion) -> TransportFrame {
    let error: Error = v2::Error::unsupported_protocol_version(requested, &SUPPORTED).into();
    TransportFrame::Single(RawJsonRpcMessage::Response(RawJsonRpcResponse::new(
        id,
        Err(Box::new(RawJsonRpcError::from(error))),
    )))
}
