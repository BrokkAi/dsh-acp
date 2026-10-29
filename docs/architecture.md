# Architecture

## Process model

`dsh-acp` is an ACP server for one connection at a time. It owns session
state, the DeepSeek HTTP client, MCP connections, and tool execution. Logs and
tracing go to stderr.

By default the connection is stdio: stdout carries JSON-RPC only, and the
process ends with it. With `--listen`, the process instead serves the SDK's
streamable HTTP and WebSocket server (`agent-client-protocol-http`) at `/acp`
until it is stopped, and serves the connections it accepts one after another.
A middleware in front of the SDK router checks `Host`, refuses any `Origin`,
and requires the bearer token before a request reaches the SDK; ACP traffic
never sees a request that fails.

Each connection gets its own `Harness`: client capabilities, the initialized
flag, provider routing (`providers/set`, `providers/disable`, logout), and a
session store with nothing in memory over the shared session files. A
connection therefore starts exactly as a new stdio process would, and its
memory is its own: loaded sessions, running turns, MCP servers, "allow always"
approvals, and pending steering. When a connection ends, its turns, commands,
and MCP servers are dropped and killed with it; what it saved stays on disk
for the next connection to load or resume.

The server holds one connection slot. An `initialize` POST without an
`Acp-Connection-Id`, or a WebSocket upgrade, waits up to two seconds for a
closing connection to release it and otherwise gets `409 Conflict`. The SDK
creates the connection through a factory that claims the slot, so two requests
that pass that check together cannot both connect: the second gets an agent
that answers every request with an error. A connection releases the slot when
it ends: on `DELETE`, when its WebSocket closes, when `initialize` fails, or,
for HTTP, after 30 seconds without an open event stream. An HTTP client
receives everything through those streams, so one without any for that long is
gone; the SDK does not detect that by itself.

Without `RUST_LOG`, the log filter is `dsh_acp=info,warn`, or
`dsh_acp=trace,info` with `--trace`, and it drops the SDK's events that carry
a `handler` field. Those describe the SDK's whole handler chain, about 1 MB
for each rejected message, which can fill a stderr pipe the client does not
drain. The SDK still logs each error response it sends, with its method and
id. `RUST_LOG` replaces the default filter entirely. Hand-rolled request
handlers answer invalid params with `respond_with_error`. Returning the error
gets it answered too, but through the SDK's handler-error path, which logs
that warning.

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

URL elicitations awaiting a completion report are connection state, not session
data. The store keeps them in memory, keyed by `elicitationId` with the session
that requested each, and drops a session's entries when it is closed or
deleted. They are never written to the session file.

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

- One ACP connection is served at a time, so a session's updates and
  agent-to-client requests go only to the connection that runs its turn, and
  no two connections run turns on one session. Two processes sharing a data
  directory are not coordinated.
- A session has one active foreground turn.
- Session transitions and final idle reporting use a per-session turn lock.
- Steering requests for a session serialize through that same lock.
- Closing prevents new prompt insertion and waits for active work to settle.
- Calls to a stdio or HTTP MCP server are serialized per server. An
  MCP-over-ACP request stands alone, so it takes no lock. A turn does not hold
  the session's state lock while it waits on an MCP server, so
  `session/cancel` and other requests for the session are not held up by a
  slow or unresponsive server.
- Session lifecycle requests run in the SDK's dispatch loop, so no other
  incoming message is handled until they answer. Connecting their MCP servers
  is bounded per server by `--mcp-connect-timeout-secs`. Servers connect one
  after another, so a request with N servers can wait up to N times that
  limit.
- MCP-over-ACP servers are not contacted during a lifecycle request. The
  client's answer to an `mcp/message` arrives through the same dispatch loop,
  so waiting for it there would never finish. The servers keep the connection
  of the lifecycle request that declared them, and the session's turns send
  `mcp/message` requests over it from outside the loop: the per-round tool
  refresh lists the tools, and dropping a request when the turn is cancelled
  sends the client `$/cancel_request`.
- Session-store writes are serialized process-wide. Loading a stored session
  takes the same lock, so a title `session/set_title` writes to a stored
  record cannot be lost to a load in progress.

## Security boundaries

Workspace roots are canonicalized and checked before tool access. Writable paths
check the nearest existing ancestor before creating files. `web_fetch` follows
redirects itself, rejects every address in a DNS answer unless it is globally
routable, and pins the validated addresses as the request resolver. It bypasses
ambient proxy configuration so the checked address is the connected address.
Provider and MCP headers are supplied by configuration and are not persisted as
session data. Instruction files are canonicalized against their source root,
limited to regular files, and capped at 1 MiB.

`--listen` binds loopback unless `--listen-allow-remote` is given. Every request
needs the bearer token, compared in constant time. The token comes from
`DSH_ACP_LISTEN_TOKEN` or is generated from the operating system's random
source; it is printed once to stderr only when generated, and is removed from
the process environment before any thread starts, so commands and MCP servers
do not inherit it. `Host` must be a loopback name, the bound address, or, with
`--listen-allow-remote`, the configured host name. Requests carrying `Origin`
are refused, the SDK's CORS support stays disabled, and its health endpoint is
off. There is no TLS.
