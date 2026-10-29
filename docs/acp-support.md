# ACP Support

This document records the protocol surfaces implemented by `dsh-acp`. It is a
project compatibility guide; the current
[Agent Client Protocol specification](https://agentclientprotocol.com/) remains
authoritative.

## Version negotiation

The server is a dual-stack ACP endpoint:

- A client requesting ACP v1 is routed to the v1 implementation.
- A client requesting ACP v2 is routed to the v2 implementation.
- A newer requested version may select the highest supported version at or
  below it.
- An unsupported version is rejected with `-32022`, whose data lists the
  requested and supported versions. A missing or malformed version is rejected
  with `-32602`. Over stdio, the server writes the error and then exits,
  because a failed `initialize` does not initialize the connection. Over HTTP
  or WebSocket, only that connection ends; over HTTP the error is the body of
  the `initialize` response, which carries no `Acp-Connection-Id`.

The version is selected once during `initialize`; subsequent traffic uses that
version's lifecycle and payload shapes.

## Baseline support

| Surface | ACP v1 | ACP v2 |
| --- | --- | --- |
| Initialize and implementation info | Yes | Yes |
| Authentication methods and logout | Yes | Yes |
| New, list, resume, close | Yes | Yes |
| Load and replay | `session/load` | `session/resume` + `replayFrom` |
| Prompt and cancellation | Yes | Async acceptance + state lifecycle |
| Tool calls and permissions | Yes | Yes |
| Rich prompt content | Text, image, resource | Text, image, resource |
| Session updates | v1 variants | v2 entity and patch variants |
| Persistent sessions | Yes | Yes |

## Optional features

| Feature | Status | Negotiation |
| --- | --- | --- |
| Session fork | Implemented | Advertised capability |
| Delete | Implemented | Advertised capability |
| Additional directories | Implemented | Advertised capability |
| System prompt append | Implemented | Advertised capability |
| System prompt override | Implemented | Advertised `mode.override` |
| Provider configuration | Implemented | Advertised provider capability |
| `auth/status` | Implemented (Draft RFD) | Advertised capability |
| MCP stdio | Implemented | Advertised transport |
| MCP streamable HTTP | Implemented | Advertised transport |
| Form elicitation | Implemented | Client capability |
| URL elicitation | Implemented, with model-reported completion | Client capability |
| `_session/steering` | Implemented | `_meta.steering.supported` |
| `session/status` | Implemented (Draft RFD) | Advertised capability |
| `session/set_title` | Implemented (Draft RFD) | Advertised capability |
| Steering idle return | Implemented | `idleBehavior: "promptRequired"` |
| Agent-owned terminals | Implemented in v2 | Baseline v2 |
| Terminal output chunks | Implemented in v2 | Baseline v2 |
| Tool content chunks | Implemented in v2 | Baseline v2 |
| Plans | Implemented | Baseline |
| Compaction | Implemented | Baseline v2; negotiated in v1 |
| Notices | Implemented for MCP refresh failures | v2 baseline; negotiated in v1 |
| Subagents | Not implemented | Draft |
| MCP-over-ACP | Implemented (draft RFD) | Advertised transport |
| ACP streamable HTTP and WebSocket transport | Implemented, opt-in | Chosen at launch with `--listen`; RFD not stable |

Audio is intentionally not advertised because the DeepSeek input API used by
this harness does not accept audio content.

## Transports

stdio is the default. `--listen <ADDR>` serves ACP at `/acp` over streamable
HTTP (POST, GET event streams, DELETE) and WebSocket instead, per the
[Streamable HTTP & WebSocket Transport RFD](https://agentclientprotocol.com/rfds/streamable-http-websocket-transport).
The RFD is in the Active stage of the RFD process, which signals current work,
not stability; the wire format may still change. The transport is the SDK's
`agent-client-protocol-http` 3.5 server, unchanged, so where that server
differs from the RFD, dsh-acp does too. For example, it accepts JSON-RPC
batches instead of answering `501`, and opens an event stream for any
`Acp-Session-Id` instead of answering `404` for one the connection does not
know. Streamable HTTP is served over HTTP/1.1 and over cleartext HTTP/2 with
prior knowledge; WebSocket upgrades use HTTP/1.1. There is no TLS and no
`Acp-Protocol-Version` handling.

dsh-acp adds checks in front of the SDK. They are not part of the RFD, which
leaves authentication to the host:

- Every request needs `Authorization: Bearer <token>` (`401` otherwise).
- `Host` must be a loopback name, the bound address, or, with
  `--listen-allow-remote`, the configured host name (`403` otherwise).
- Requests with an `Origin` header are refused (`403`).
- One ACP connection is served at a time (`409` for another).

Both protocol versions work over both transports, and a connection behaves as
a stdio connection does: it starts with nothing in memory and can load or
resume sessions that earlier connections saved. See the
[README](../README.md#http-and-websocket) for usage and the security
trade-offs of `--listen-allow-remote`.

## Session status

`session/status` follows the
[Session Status RFD](https://agentclientprotocol.com/rfds/session-status),
which is still a Draft. `agent-client-protocol` 3.5 has no types for it, so
the request, response, and capability marker are written by hand and may change
with the RFD. Both versions advertise it with an empty marker:
`agentCapabilities.sessionCapabilities.status` in v1 and
`capabilities.session.status` in v2.

The request takes a `sessionId` and reports where that session is:

| Result | Meaning |
| --- | --- |
| `{"status": "active"}` | The session is in memory in this server process. |
| `{"status": "inactive"}` | The session is stored but not in memory, for example after `session/close` or a restart. |
| `-32002` with `data.sessionId` | No such session, including a deleted one. |

A missing or non-string `sessionId` is rejected with `-32602`. In v2, an
`active` result also carries `state`, the same `running`, `requires_action`,
or `idle` snapshot that `session/resume` sends. v1 has no foreground state, so
its result has no `state`. The probe does not wait for a session's work, so it
answers promptly even while that session is in a tool or MCP call. Requests
are handled one at a time, though, and some others do wait on the session.
Until [#46](https://github.com/BrokkAi/dsh-acp/issues/46) is fixed, a
`session/cancel` or `session/resume` sent during an MCP call waits for the call
to finish, and every later request on the connection waits behind it,
including `session/status`.

The request does not load, resume, or change the session. As the RFD says,
`active` only means the session is in memory, so a client still sends
`session/load` (v1) or `session/resume` before prompting a session it did not
create or resume itself. An inactive session is found by checking that its
record file exists, without reading it, so an unreadable record is still
reported as `inactive` and fails when resumed.

## Session titles

`session/set_title` follows the
[Client-Set Session Titles RFD](https://agentclientprotocol.com/rfds/session-set-title),
which is still a Draft. `agent-client-protocol` 3.5 has no types for it, so
the request, response, and capability marker are written by hand and may change
with the RFD. Both versions advertise it with an empty marker:
`agentCapabilities.sessionCapabilities.setTitle` in v1 and
`capabilities.session.setTitle` in v2.

The request takes a `sessionId` and a `title`, and the result is the title as
stored, which clients should display:

```json
{"title": "Investigate OAuth timeout"}
```

Before storing it, the agent trims the title, turns each run of whitespace or
control characters, line breaks included, into one space, and cuts it to 256
characters (Unicode scalar values).

| Case | Answer |
| --- | --- |
| A session in memory in this process | The title, after a `session_info_update` that carries it. |
| A stored session that is not in memory, for example after `session/close` or a restart | The title. The record is updated on disk without loading the session, which stays `inactive`, so no update is sent. |
| A title that is empty after normalization | `-32602` with the message `Title must not be empty.` |
| A missing or non-string `sessionId` or `title` | `-32602` |
| No such session, including a deleted one | `-32002` with `data.sessionId` |

The title is stored before the answer is sent. The request changes only the
title: the session's `updatedAt` stays as it was, so a rename does not move
the session in `session/list`, which lists the most recently updated sessions
first.

A session without a title takes one from its first prompt. Once a client sets
a title, the record says so, and generated titles no longer replace it, also
after a restart. Another `session/set_title` still replaces it. A title can be
set while a turn runs; the turn is unaffected and keeps the new title when it
saves the session. Sessions stored by earlier versions have no such flag, so
they keep their generated titles until a client renames them.

Within one process, setting a title and loading the session are serialized,
so a load cannot bring back the old title. Another `dsh-acp` process that has
the same session in memory does not see the new title and overwrites it the
next time it saves the session. The RFD lets an agent refuse a session another
process holds, but `dsh-acp` cannot tell that one does.

## Authentication status

`auth/status` follows the
[Agent Authentication State Query RFD](https://agentclientprotocol.com/rfds/get-auth-state),
which is still a Draft. `agent-client-protocol` 3.5 has no types for it, so
the response and capability marker are written by hand and may change with the
RFD. Both versions advertise it with a `status` field in the agent's auth
capabilities. v1 sends `agentCapabilities.auth.status: true`, the boolean in
the RFD's example. The RFD shows only v1, so v2 sends
`capabilities.auth.status: {}`, because v2 capability markers are objects
(the client's `auth.terminal` capability is `true` in v1 and `{}` in v2).

The request takes no parameters other than an optional `_meta`, and the
result is `authenticated` plus a `message` for the user:

```json
{
  "authenticated": true,
  "message": "Using the DeepSeek API key from DEEPSEEK_API_KEY or --api-key."
}
```

| State | `authenticated` |
| --- | --- |
| A non-blank `DEEPSEEK_API_KEY` or `--api-key`, or a provider `Authorization` header | `true` |
| Neither of those | `false` |
| A credential, but after `logout` (v1), `auth/logout` (v2), or `providers/disable` | `false` until a successful `authenticate`, `auth/login`, or `providers/set` |

The provider `Authorization` header takes precedence over the API key, and the
message says which one requests will use. After a logout or a disabled
provider, prompts fail with `auth_required` until the client authenticates
again, so that state is reported as unauthenticated even though the key is
still configured.

As the RFD allows, `true` means a credential is configured, not that DeepSeek
accepts it; the check is local and makes no request. The query can be sent at
any time after `initialize`, including before authenticating. It changes
nothing and never returns the key or header values.

## Session configuration options

Both protocol versions expose select options for `model`, `mode`,
`permission_mode`, and `reasoning_effort`, plus a boolean `network` toggle.
`model`, `mode`, and `reasoning_effort` carry the ACP `model`, `mode`, and
`thought_level` categories; `permission_mode` is uncategorized. The thinking
effort carries `none`, `low`, `high`, or `max`, is stored with the session, and
is sent to DeepSeek as `reasoning_effort`.

The v1 prompt response and the v2 idle state report only the completed turn's
token usage, with `_meta["mjolnir.dev/usage-scope"] = "turn"`. Session
`usage_update` notifications continue to report the cumulative session total.

## Client system prompts

Both protocol versions advertise the unstable `systemPrompt` session
capability with `mode.override`: `agentCapabilities.sessionCapabilities` in v1
and `capabilities.session` in v2. The client's prompt is accepted on
`session/new`, `session/resume`, and `session/fork`, and in v1 also on
`session/load`.

- `append` adds the client's text at the end of the system prompt, after the
  built-in harness guidance, the `--system-prompt` text, and the `AGENTS.md`
  files.
- `override` replaces the built-in harness guidance and nothing else. The
  `--system-prompt` text and the `AGENTS.md` files still follow the client's
  text, and plan mode still adds its instructions. Tools, MCP servers,
  permissions, and session configuration are unchanged.

Only text content blocks are accepted. A request with another block type or a
mode other than `append` or `override`, including `_`-prefixed modes, fails
with `-32602` (Invalid params) and creates, loads, resumes, or forks nothing.

The prompt and its mode are stored with the session. A load, resume, or fork
that carries `systemPrompt` uses that prompt and mode. One that omits it keeps
the stored ones, and a fork inherits its source's. A load or resume whose MCP
servers fail to connect leaves the session's prompt, mode, and additional
directories as they were.

## Prompt lifecycle

ACP v2 responds to `session/prompt` after inserting the user message, before
model work completes. Foreground state is reported through `state_update` and
ends with an idle snapshot and stop reason.

ACP v1 retains its request-scoped prompt response: the response is sent when
the turn completes and carries the stop reason directly.

Both versions persist user content, assistant output, tool calls, outcomes,
plans, compactions, and terminal/diff state for replay.

## Request cancellation

ACP v1 honors `$/cancel_request` for a `session/prompt` that is still running.
The turn stops the way it does for `session/cancel`: the model stream and the
running tool stop, unfinished tool calls are reported and saved as failed, and
a request still waiting on the client, such as a permission request, is
cancelled with its own `$/cancel_request`. The prompt is then answered once:
with error `-32800` (Request cancelled) when `$/cancel_request` alone stopped
the turn, or with its normal result if the turn had already finished. If
`session/cancel` or `session/close` also reaches the turn before the prompt is
answered, the prompt ends with `stopReason: "cancelled"` as usual, which the
request cancellation rules accept too.

ACP v2 answers `session/prompt` once the user message is inserted, so a later
`$/cancel_request` cannot stop the work it started; use `session/cancel`.

Every other request, in both versions, is answered before a later
`$/cancel_request` is read, so the notification has no effect on it.

## Permissions and workspace boundaries

Reads, writes, searches, local commands, and MCP calls execute in the agent.
The session `cwd` and additional roots define the filesystem boundary. Sensitive
tools request permission unless the session policy or an explicit remembered
decision allows them. Every `web_fetch` call requires permission; a remembered
decision covers only the requested origin. Cross-origin redirects are refused
so a redirect cannot escape that decision. Reads of credential-like filenames
and directories, including a resolved symlink target, require a separate
file-scoped permission decision. Searches skip those paths, and a search whose
root is itself credential-like is refused.

Workspace reads, edits, and searches enforce the byte and entry limits listed
in the README. MCP responses and DeepSeek streams are likewise bounded while
being read; exceeding a limit fails the request without first buffering the
whole input.

A permission request that the Client answers with an error grants nothing. In
both versions the tool does not run, its call fails with the reason, and the
turn continues, as after a denial. That includes a Client that cancels the
permission request itself and answers `-32800`. Only the turn's own
cancellation ends the turn: `session/cancel`, after which the Client answers
pending permission requests with the `cancelled` outcome, or in ACP v1 a
`$/cancel_request` for the prompt.

In plan mode, read-only MCP tools marked with `annotations.readOnlyHint: true`
remain available and may be called. Other MCP tools are hidden and refused
before permission is requested.

A write or edit shows its change before it runs: the pending tool call and the
permission request carry a diff of the whole file (old and new text in v1, a
patch in v2). When the change cannot be worked out, for example because the
text to replace is not in the file, they carry no diff and the tool reports
the error when it runs.

File, directory, and search tools use local capability-confined operations in
both protocol versions, so they do not re-resolve a workspace root path after
approval. ACP v1 may still delegate terminal execution to a capable client. A
command that runs in the client's terminal puts that terminal in its tool call
as soon as the client creates it. The final update keeps the terminal and adds
the result text only if the command failed; replay after `session/load` shows
the result text, because the terminal has been released.
ACP v2 has no client filesystem or terminal execution methods; use an MCP server,
such as an MCP-over-ACP server, for client-provided tools.

## Elicitation

`ask_user` sends a session-scoped `elicitation/create` tied to its tool call.
It uses form mode, or URL mode when the model passes a URL and the Client
advertises URL elicitation. A URL must use HTTPS outside localhost and must not
embed credentials. In both versions, a request refused before it is sent, or
answered with an error, fails that tool call with the reason and the turn
continues. Only the turn's own cancellation ends the turn: `session/cancel`, or
in ACP v1 a `$/cancel_request` for the prompt. A Client that cancels the
elicitation request itself answers `-32800`; that fails only the tool call, and
a URL elicitation's ID is freed as it is for a decline. ACP v2 reports
`requires_action` only once the request has been sent.

Accepting a URL elicitation means only that the user consented to open the
URL. The harness has no callback that observes the external interaction, so the
model reports completion. The `ask_user` result says completion was not
verified and names the `elicitationId`. Clients that advertise URL elicitation
are also offered `complete_url_elicitation`; once the model has verified the
interaction finished, for example by retrying the operation that needed it,
that tool sends `elicitation/complete` with the original ID on the same
connection. It asks no permission and stays available in plan mode. An ID that
is unknown, was declined or cancelled, was already completed, or belongs to
another session fails the tool call and sends nothing.

Each `elicitationId` is unique among the connection's outstanding URL
elicitations, so `ask_user` refuses a model-chosen ID that is still
outstanding: the tool call fails and tells the model to omit the ID so one is
generated. Accepted IDs are held in memory for the connection and never
written to disk. They survive `session/cancel`, later turns, and `session/load`
or `session/resume` on the same connection, because the external interaction
may still finish. If a turn is cancelled while its `elicitation/create` is
still unanswered, the Client may already have accepted it, so the ID stays
reserved, but can never be completed, until the session is closed or deleted.
Pending and reserved IDs are dropped without a completion when the session is
closed or deleted, or when the process exits. Replay does not resend
`elicitation/complete`, and a fork does not inherit the source session's
pending IDs.

## MCP-over-ACP

MCP-over-ACP lets the client serve MCP tools over the ACP connection itself.
It follows the [MCP-over-ACP RFD](https://agentclientprotocol.com/rfds/mcp-over-acp),
which is a draft and may still change. Both protocol versions advertise it:
`agentCapabilities.mcpCapabilities.acp: true` in v1 and
`capabilities.session.mcp.acp: {}` in v2.

The client declares a server with `"type": "acp"`, a `name`, and the
`serverId` it generated, in `session/new`, `session/resume`, `session/fork`,
or v1 `session/load`. A request that declares one `serverId` twice fails with
`-32602`. The client of the connection that sent the declaration serves the
server. A later load or resume uses the servers that request declares, as for
other transports. A v2 `session/resume` that omits `mcpServers` keeps the
session's current servers.

Each MCP operation is one `mcp/message` request to the client, in the
connection's protocol version. It targets MCP revision 2026-07-28 only and
carries a fresh `requestId` and its own `_meta`: the protocol version, empty
`clientCapabilities`, and `clientInfo`. There is no MCP `initialize`. The agent
sends `tools/list`, following `nextCursor`, and `tools/call`.

The server is not contacted while a session is set up. A lifecycle request is
answered from the connection's ordered dispatch, so the client's reply to a
`tools/list` could not be read until the request had returned. Instead, the
tools are listed at the start of each model round, as other MCP servers' tools
are refreshed. `--mcp-connect-timeout-secs` therefore does not apply. The tools
are named `mcp__<name>__<tool>`, hidden in plan mode unless they are marked
`annotations.readOnlyHint: true`, and ask permission like other MCP tools.

Tools are listed server by server. A server whose listing fails keeps the
tools its last listing found, or none if it was never listed, so it cannot
hide other servers' tools. A server the client reports it no longer serves
(`-33001`) loses its tools. The refresh notice names each server that failed.

Each of these fails only the tool call, or that server's tool listing, which
is reported as a notice. None ends the turn:

- an ACP error response to `mcp/message`, such as `-33001` for a `serverId`
  the client does not serve;
- an MCP error inside a successful response. Its code is MCP's, so a code
  that equals an ACP one, such as `-32800` or `-32000`, means nothing to ACP;
- an `input_required` result. The agent declares no MCP client capabilities
  and does not answer multi round-trip requests.

A tool result with `isError: true` is reported as a failed tool call.

Cancelling the turn withdraws an outstanding `mcp/message` with
`$/cancel_request` for its ACP request ID, and the turn ends without waiting
for the client to answer. The agent sends no progress token. It ignores
`mcp/message` notifications from the client.
