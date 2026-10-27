# Architecture

## Process model

`dsh-acp` is one stdio ACP server. It owns session state, the DeepSeek HTTP
client, MCP connections, and tool execution. ACP stdout carries JSON-RPC only;
logs and tracing go to stderr.

The SDK protocol router selects the v1 or v2 implementation from the first
`initialize` request. Shared application logic lives behind that boundary so
wire-specific behavior does not leak across versions.

## Session runtime

Each session contains:

- immutable identity, working directory, and additional roots;
- model, mode, permission, thinking effort, and network configuration;
- effective system instructions loaded from configuration, `AGENTS.md`, and
  the ACP client;
- conversation messages and original ACP prompt blocks;
- tool outcomes, raw output, diffs, and terminal snapshots;
- plans, compactions, message IDs, and pending steering input;
- foreground activity flags and cancellation state.

JSON writes use a temporary file and atomic rename. Session files live under
`<data-dir>/sessions/` and are created with owner-only permissions on Unix.

## Prompt execution

The DeepSeek request context is bounded by the configured character budget.
The model may emit text, reasoning, and function calls. Tool calls are
assembled from streamed fragments, assigned session-unique IDs, authorized,
executed, persisted, and returned to the model until the model completes or the
user cancels the turn. A turn has no round limit.

One completion is requested with up to three attempts. A rate limit or 5xx is
retried with backoff; connection failures and responses that end without a
`finish_reason` are retried too. Retrying stops as soon as text or reasoning
has reached the client, because a second attempt would duplicate what the user
already saw. An empty body is never accepted as an answer. The HTTP client
bounds connecting and stalled reads rather than total request time, so a long
stream that keeps producing tokens is not interrupted.

A completion that ends with `finish_reason: "length"` is not a finished answer.
The turn stops with `max_tokens`, and any tool call in that completion is marked
failed rather than executed, because its arguments were cut off mid-string.
Clients that advertise notice support also receive a warning describing the
truncation.

ACP v2 reports foreground state independently from prompt acceptance. Cancelling
active work stops the model stream and tool operations, emits final tool state,
and settles to idle with the `cancelled` reason. Idle usage covers only that
foreground turn; per-update usage continues to track the session total.

An ACP v1 prompt turn runs outside the connection's ordered dispatch, so a
`$/cancel_request` for the prompt reaches it while it runs. The notification
cancels a child of the token `session/cancel` uses, so the turn stops the same
way and the session token still shows whether `session/cancel` or a close
reached the turn. A turn stopped by `$/cancel_request` alone answers the prompt
with `-32800`; one that `session/cancel` also reached ends with
`stopReason: "cancelled"`. Every other request is answered from the ordered
dispatch, before a later `$/cancel_request` can be read.

## Replay

Full replay rebuilds retained conversation entities in order. Tool calls retain
their terminal status, diff text and structured changes, terminal snapshots,
and raw output. Plans and compactions are emitted as their normal ACP update
types. Original prompt blocks are replayed when available rather than showing a
lossy text reconstruction.

## Concurrency

- A session has one active foreground turn.
- Session transitions and final idle reporting use a per-session turn lock.
- Steering requests for a session serialize through that same lock.
- Closing prevents new prompt insertion and waits for active work to settle.
- MCP calls are serialized per server.
- Session-store writes are serialized process-wide.

## Security boundaries

Workspace roots are canonicalized and checked before tool access. Writable paths
check the nearest existing ancestor before creating files. Web access rejects
private and local destinations, including via redirects. Provider and MCP
headers are supplied by configuration and are not persisted as session data.
Instruction files are canonicalized against their source root, limited to
regular files, and capped at 1 MiB.
