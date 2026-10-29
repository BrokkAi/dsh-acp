//! `session/set_title`, from the draft
//! [Client-Set Session Titles RFD](https://agentclientprotocol.com/rfds/session-set-title).
//!
//! agent-client-protocol 3.5 has no types for this method, so its request and
//! result are hand-rolled here until the SDK has them. Both protocol versions
//! store the title the same way; each sends its own `session_info_update`.
//! Each version's `initialize` adds the `setTitle` session capability with
//! `agent::with_extra_capabilities`.

use agent_client_protocol::{Error, ErrorCode};
use serde_json::Value;

use crate::{agent::Harness, session::SessionPresence, session_status};

pub(crate) const METHOD: &str = "session/set_title";

/// The longest title stored, in characters. claude-agent-acp, which the RFD
/// cites, caps titles at the same length.
const MAX_TITLE_CHARS: usize = 256;

/// A title `session/set_title` stored.
pub(crate) struct StoredTitle {
    pub(crate) session_id: String,
    /// The title after normalization, which the result reports.
    pub(crate) title: String,
    /// Whether the session is in memory, and so loaded on this connection,
    /// the only one this process serves.
    pub(crate) loaded: bool,
}

impl Harness {
    /// Validates a `session/set_title` request and stores its title, whether
    /// or not the session is loaded.
    pub(crate) async fn store_session_title(&self, params: &Value) -> Result<StoredTitle, Error> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::invalid_params().data("session/set_title requires a string sessionId")
            })?;
        let title = params.get("title").and_then(Value::as_str).ok_or_else(|| {
            Error::invalid_params().data("session/set_title requires a string title")
        })?;
        let title = normalize_title(title);
        if title.is_empty() {
            // The RFD's example message.
            return Err(Error::new(
                ErrorCode::InvalidParams.into(),
                "Title must not be empty.",
            ));
        }
        let loaded = match self.store.set_client_title(session_id, &title).await? {
            SessionPresence::Active(_) => true,
            SessionPresence::Inactive => false,
            SessionPresence::Unknown => return Err(session_status::unknown_session(session_id)),
        };
        Ok(StoredTitle {
            session_id: session_id.to_string(),
            title,
            loaded,
        })
    }
}

/// Trims the title, turns each run of whitespace and control characters,
/// line breaks included, into one space, and cuts it to `MAX_TITLE_CHARS`.
fn normalize_title(title: &str) -> String {
    let title = title
        .split(|c: char| c.is_whitespace() || c.is_control())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    match title.char_indices().nth(MAX_TITLE_CHARS) {
        Some((end, _)) => title[..end].trim_end().to_string(),
        None => title,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn titles_are_trimmed_collapsed_and_capped() {
        assert_eq!(
            normalize_title("  Investigate\r\n\tOAuth \u{a0} timeout  "),
            "Investigate OAuth timeout"
        );
        assert_eq!(
            normalize_title("bell\u{7}and\u{1b}[31mescape"),
            "bell and [31mescape"
        );
        for blank in ["", "   ", "\n\t\r", "\u{0}\u{7f}"] {
            assert_eq!(normalize_title(blank), "", "{blank:?}");
        }

        // Characters, not bytes.
        let long = "ü".repeat(MAX_TITLE_CHARS + 10);
        assert_eq!(normalize_title(&long), "ü".repeat(MAX_TITLE_CHARS));
        let exact = "a".repeat(MAX_TITLE_CHARS);
        assert_eq!(normalize_title(&exact), exact);
        // A cut that ends on a space drops it.
        let words = format!("{} tail", "a".repeat(MAX_TITLE_CHARS - 1));
        assert_eq!(normalize_title(&words), "a".repeat(MAX_TITLE_CHARS - 1));
    }
}
