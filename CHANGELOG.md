# Changelog

All notable changes to `dsh-acp` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.5.0 - 2026-10-27

### Added

- MCP-over-ACP (draft RFD): the ACP client can serve MCP tools over the ACP
  connection itself. Both protocol versions advertise the `acp` MCP transport
  (`mcpCapabilities.acp: true` in v1, `session.mcp.acp: {}` in v2) and accept
  `{"type": "acp", "name", "serverId"}` declarations wherever MCP servers are
  declared. Each MCP operation is an `mcp/message` request to the client,
  using MCP 2026-07-28 with per-request metadata and no `initialize`
  handshake. The server is not contacted during session setup; its tools are
  listed at the start of each model round and named, filtered in plan mode,
  and permission-checked like other MCP tools. An ACP or MCP error fails only
  the tool call or that server's tool listing, and cancelling the turn
  withdraws an outstanding request with `$/cancel_request`. Declaring one
  `serverId` twice in a request fails with `-32602`.
- MCP tools are now refreshed server by server. A server whose `tools/list`
  fails keeps its last listed tools instead of hiding every server's tools
  for that round, and the refresh notice names the servers that failed.
- ACP clients can now replace the built-in harness prompt instead of only
  appending to it: both protocol versions advertise
  `systemPrompt.mode.override` and accept `mode: "override"` on session
  create, load (v1), resume, and fork. Only the harness's own guidance is
  replaced. The `--system-prompt` text, `AGENTS.md` instructions, and plan-mode
  instructions still follow the client's text, and tools, MCP servers,
  permissions, and workspace boundaries are unchanged. The mode is stored with
  the session, so a load, resume, or fork that omits `systemPrompt` keeps it.
  Append mode behaves as before, and sessions stored by earlier versions keep
  appending.
- ACP v1 now honors `$/cancel_request` for an in-flight `session/prompt`.
  Previously the notification was ignored and the turn ran to completion. The
  turn now stops the way it does for `session/cancel`: the model stream and
  the running tool stop, unfinished tool calls are reported and saved as
  failed, and a request still waiting on the client, such as a permission
  request, is cancelled too. The prompt is answered once: with error `-32800`
  (Request cancelled) when `$/cancel_request` alone stopped the turn, or with
  its normal result if the turn had already finished. A prompt that
  `session/cancel` reaches still ends with `stopReason: "cancelled"`, even if
  a `$/cancel_request` for it arrives as well. ACP v2 is unchanged:
  `session/prompt` is answered when the message is inserted, so stopping the
  work it started still takes `session/cancel`.
- `--mcp-connect-timeout-secs` (`DSH_ACP_MCP_CONNECT_TIMEOUT_SECS`, default
  60) sets how long each MCP server may take to start, complete the MCP
  handshake, and list its tools when a session is created, loaded, resumed, or
  forked. Servers connect one after another, so a request that declares
  several servers can take up to that many times the limit. Tool calls and the
  tool list refresh during a turn are not bounded by it.
- `session/status` from the Draft Session Status RFD, in both protocol
  versions. It reports whether a session is `active` (in memory) or `inactive`
  (stored, for example after `session/close` or a restart) without loading,
  resuming, or changing it, and answers an unknown or deleted session with
  `-32002` and the requested `sessionId`. A v2 `active` result also carries the
  session's `running`, `requires_action`, or `idle` state. It is advertised as
  `sessionCapabilities.status` in v1 and `session.status` in v2. The SDK has no
  types for the method yet, so its shape follows the RFD and may change with
  it.
- `auth/status` from the Draft Agent Authentication State Query RFD, in both
  protocol versions. It tells a client whether a DeepSeek credential is
  configured (`DEEPSEEK_API_KEY`, `--api-key`, or a provider `Authorization`
  header) without logging in or changing anything, and reports `false` after a
  logout or a disabled provider until the client authenticates again. It never
  returns the credential. It is advertised as `agentCapabilities.auth.status`
  in v1 and `capabilities.auth.status` in v2. The SDK has no types for the
  method yet, so its shape follows the RFD and may change with it.
- URL elicitations can now be closed out. Accepting a URL only means the user
  agreed to open it, and the harness has no callback that sees the external
  step finish, so `elicitation/complete` was never sent and the Client could not
  tell the flow was over. The `ask_user` result now gives the model the
  elicitation ID, and Clients that advertise URL elicitation are offered a
  `complete_url_elicitation` tool. Once the model has verified the step
  finished, for example by retrying the operation that needed it, the tool sends
  `elicitation/complete` with the original ID on the same connection. Unknown,
  declined, already completed, and other sessions' IDs fail the call and send
  nothing. Pending IDs live in memory only and are dropped, without a
  completion, when the session is closed or deleted.
- `session/set_title` from the Draft Client-Set Session Titles RFD, in both
  protocol versions. A client can rename a session whether or not it is
  loaded; a stored session is updated on disk without being loaded, and for a
  session in memory the client is also sent a `session_info_update`. The
  title is trimmed, runs of whitespace and control characters become one
  space, it is capped at 256 characters, and the result returns the title as
  stored. Once a client sets a title, titles generated from prompts no longer
  replace it, also after a restart. A title that is empty after normalization
  is rejected with `-32602`, and an unknown or deleted session with `-32002`.
  It is advertised as `sessionCapabilities.setTitle` in v1 and
  `session.setTitle` in v2. The SDK has no types for the method yet, so its
  shape follows the RFD and may change with it.

### Changed

- HTTP MCP server setup is now bounded by `--mcp-connect-timeout-secs`, 60
  seconds in total by default. It was bounded only by a fixed 180-second
  timeout on each request (`initialize`, `notifications/initialized`, and each
  `tools/list` page), so an HTTP MCP server that takes more than 60 seconds to
  connect now fails session setup unless the limit is raised.
- The result of an accepted URL elicitation no longer includes `content` the
  Client attached to its response. What the user enters out of band must stay
  out of the model's context.

### Fixed

- An `initialize` request with an unsupported or malformed `protocolVersion`
  now always gets its error response (`-32022` with the requested and
  supported versions, or `-32602`) before the server exits. The response was
  often lost, most often while the client still had stdin open. The same fix
  delivers the `-32600` error for a first request that is not `initialize`, and
  the `-32700` error for a first line that is not JSON.
- `session/load` and `session/resume` no longer fail with `-32800` (request
  cancelled) when they connect MCP servers after the session received
  `session/cancel`. Connecting used the last turn's cancellation token, which
  stays cancelled until the next prompt, so a cancelled turn, or a cancel sent
  while the session was idle, aborted it.
- A `session/load` or `session/resume` that failed because an MCP server did
  not connect still applied the request's system prompt and additional
  directories, and a v1 load had already saved them to the session file. The
  next prompt used instructions from a request the client was told had failed.
  Both versions now install them only after the MCP servers connect.
- A stdio MCP server that started but never answered the MCP handshake or its
  first `tools/list` hung `session/new`, `session/load`, `session/resume`, or
  `session/fork`, and with it every later message on the connection. Such a
  server now fails the request once `--mcp-connect-timeout-secs` passes, with
  an error naming the server, and the stdio server processes started for the
  request are killed.
- `session/cancel` now stops a turn that is waiting on an MCP server. The turn
  held the session's state lock during an MCP `tools/call` and during the
  `tools/list` refresh at the start of each round, and `session/cancel` needs
  that lock, so the cancel waited until the MCP server answered: up to the
  180-second HTTP timeout, or indefinitely for a stdio server that never
  replies. The connection handles messages in order, so every later message
  waited too, and requests that use the session's state, such as
  `session/set_mode`, waited even without a cancel. The turn now takes the lock
  only to look up the session's MCP servers, so the cancel stops the MCP
  request at once. An interrupted MCP tool call is reported as failed, an ACP
  v1 prompt ends with `stopReason: "cancelled"`, and ACP v2 settles to idle
  with `cancelled`.
- An empty or whitespace-only `DEEPSEEK_API_KEY` or `--api-key` was sent to
  DeepSeek as a blank bearer token, so the prompt failed with DeepSeek's HTTP
  401 even though authentication already treated such a key as missing. It now
  counts as no key: the prompt fails locally with the `-32000` error that says
  to set `DEEPSEEK_API_KEY`, and no request is sent. An `Authorization` header
  set with `providers/set` is still used in its place.
- `ask_user` sent a model-chosen URL `elicitation_id` again even while an
  earlier request with that ID was still outstanding, which ACP forbids. It now
  fails the tool call and tells the model to omit the ID so one is generated.
  An ID whose request was cancelled before the Client answered stays reserved
  until the session closes, because the Client may already have accepted it.
- An `ask_user` request that could not be sent, or that the Client answered
  with an error, did not tell the model why. In ACP v1 the model saw only the
  JSON-RPC error name, such as "Invalid params". In ACP v2 the whole turn ended
  with stop reason `error`, and the model saw only "Tool call failed before
  completion." Both versions now fail just that tool call with the reason, for
  example that the URL must use HTTPS, and the turn continues. In ACP v2, a
  request refused before it is sent no longer reports `requires_action`.
- A Client that cancelled an `ask_user` elicitation itself, answering `-32800`,
  ended the whole turn as `cancelled` although nobody cancelled the turn. Only
  the turn's own cancellation (`session/cancel`, or a v1 `$/cancel_request` for
  the prompt) now ends it; a Client-side cancel fails only that tool call, and
  the model is told the client cancelled the request.
- In ACP v2, an `update_plan` call without an `entries` array, or with an entry
  that has no `content`, ended the whole turn with stop reason `error`, and the
  model saw only "Tool call failed before completion." It now fails just that
  tool call with the reason, the turn continues, and the model can send a
  corrected plan.
- A call to an `mcp__` tool in a session with no MCP servers ended the whole
  turn: an ACP v1 `session/prompt` failed with `-32603`, and an ACP v2 turn
  ended with stop reason `error`. In both versions it now fails like a call to
  any other unknown MCP tool, and the turn continues.
- Each request or notification rejected for invalid params, such as
  `session/new` with a non-string `cwd` or `_session/steering` without a
  `sessionId`, wrote about 1 MB to stderr. The SDK's `Handler errored` warning
  describes every registered handler, escaped again for each one. A client
  that piped stderr without reading it stalled after the first rejection: the
  server blocked writing the log and stopped answering. The default log filter
  now drops the SDK's warnings that describe its handler chain, so a rejection
  logs one line of a few hundred bytes, the SDK's warning for the error
  response it sends. `_session/steering` now answers invalid params itself,
  like the other hand-rolled methods. A `RUST_LOG` setting still replaces the
  default filter.
- A Client that answered a permission request with an error ended the whole
  turn: as `cancelled` for `-32800` (Request cancelled) although nobody
  cancelled the turn, and otherwise with a prompt error in ACP v1 or stop
  reason `error` in ACP v2. The model was never told why the tool did not run.
  Both versions now treat the error as a refusal: the tool does not run, its
  call fails with the reason, for example that the client cancelled the
  permission request, and the turn continues. Only the turn's own cancellation
  (`session/cancel`, or a v1 `$/cancel_request` for the prompt) ends it.

## 0.4.0 - 2026-10-25

### Added

- The system prompt now includes `AGENTS.md` from `<data-dir>/AGENTS.md`, the
  session working directory, and each additional workspace root. Repository
  and user instructions are rebuilt on session create, load, resume, and fork,
  so long-lived clients no longer need to pass them through an environment
  variable or ACP request. Instruction files are confined to their source root,
  must be regular files, and are limited to 1 MiB.

### Changed

- Removed `--temperature` and `DSH_ACP_TEMPERATURE`. DeepSeek ignores sampling
  parameters in thinking mode, which is enabled by default, so the setting was
  inert for most sessions.

### Fixed

- ACP v2 idle usage now reports the completed foreground turn's own tokens
  instead of the session's running total, and declares
  `_meta["mjolnir.dev/usage-scope"] = "turn"`. A local command reports zero.
  `usage_update` still carries the session total.
- `session/resume` now reads the client-provided system prompt in both protocol
  versions instead of leaving the instructions from the previous lifecycle
  request in place.

## 0.3.0 - 2026-10-24

### Added

- Thinking effort is now configurable per session, through the `--reasoning-effort`
  flag and `DSH_ACP_REASONING_EFFORT` environment variable, the "Thinking effort"
  ACP config option in both protocol versions (category `thought_level`), and the
  `/effort` command. It is sent to DeepSeek as `reasoning_effort` with the values
  `none`, `low`, `high`, and `max`, defaulting to `high` as DeepSeek does.
  Sessions stored before this change default to `high`. `none` turns thinking
  mode off and is cheapest. DeepSeek counts the chain of thought inside
  `max_tokens`, so an explicitly bounded `--max-tokens` may need to be larger
  when using `max`.

### Changed

- A prompt is no longer capped at a fixed number of model/tool round trips.
  The old limit (24, `--max-tool-rounds`) ended a turn mid-task with
  `max_turn_requests`, marked the remaining tool calls failed without a
  message, and sent the client nothing to explain it, which read as the
  session exiting early on its own. A turn now runs until the model stops
  asking for tools or the user cancels it. The `--max-tool-rounds` flag and
  `DSH_ACP_MAX_TOOL_ROUNDS` environment variable are removed.

### Fixed

- A response that ended at the token limit was treated as a finished answer: any
  tool call it carried was executed even though its arguments had been cut off
  mid-string, and the turn ended with `max_tokens` and no explanation. Such a
  response now stops the turn, marks those tool calls failed instead of running
  them, and reports a warning notice when the client supports notices.
- The default `--max-tokens` was 8192, about 2% of what DeepSeek allows, so a
  large file write or a long chain of thought hit `max_tokens` and truncated
  the answer. The harness no longer sends `max_tokens` at all, matching
  DeepSeek's own examples, so the API uses its maximum of 384000 output tokens.
  `--max-tokens` remains available to bound cost.
- The reported context window was 128K, while both models accept 1M tokens, so
  the context gauge read as full long before it was. The default conversation
  budget (`--context-chars`) rises from 400000 to 2000000 characters, roughly
  600000 tokens, leaving room for the response.
- A completion that was still streaming tokens at the request deadline was
  killed mid-answer: `reqwest`'s total timeout also bounded the response body,
  and the error claimed the API "did not answer in time" when it was answering
  fine. The client now bounds connecting and stalled reads instead, so a long
  answer completes.
- A 200 with an empty or truncated event-stream was reported as `end_turn`, so
  a dropped upstream connection looked like the model simply stopped and a
  half-written file looked like a finished task. A response now has to carry a
  `finish_reason` or `[DONE]`, and an empty body is never an answer.
- Rate limits and 5xx responses ended the turn without a retry. They are now
  retried up to three attempts with exponential backoff, honouring
  `Retry-After`. Retries stop once text has reached the client, so a partly
  streamed answer is reported instead of being duplicated.

## 0.2.3 - 2026-10-23

### Changed

- Plan mode now offers and allows MCP tools that declare `readOnlyHint: true`.
  Other MCP tools remain hidden and are refused before permission is requested.

## 0.2.2 - 2026-10-23

### Changed

- A write or edit now shows the change it will make before the user approves
  it. Its pending tool call and permission request carry a diff of the whole
  file, worked out without writing anything; they used to carry only a title
  such as "edit file calc.py". When the change cannot be worked out (the file
  is missing or is not text, the text to replace is not found or occurs more
  than once, or the path is outside the workspace), the request carries no
  diff, as before, and the tool reports its own error when it runs. ACP v1
  sends the old and new text; ACP v2 sends a patch.

### Fixed

- Under ACP v1, a command that runs in the client's terminal now puts that
  terminal in its own tool call, so the client shows the live output there.
  No tool call used to contain the terminal, and a client that gives such a
  terminal a tool call of its own, as Mjolnir does, showed every command
  twice. The final update keeps the terminal and no longer repeats the output
  as text, except when the command failed; `rawOutput` still carries the exit
  status and output. Replay after `session/load` shows the output as text,
  because a released terminal cannot be shown again.

## 0.2.1 - 2026-10-23

### Fixed

- Plan mode now tells the model it is planning and offers it only tools that
  cannot change the workspace. A write, edit, command, or MCP call the model
  asks for anyway is refused before any permission request, so the user is no
  longer asked to approve a change that plan mode then rejects.
- A command's tool title and permission request quote its arguments as a
  shell would, so `sh -c 'ls && rm x'` no longer reads as `sh -c ls` followed
  by a second command. ACP v2 titles now include the arguments.
- "Allow for this session" is scoped as Codex scopes it: a command is
  remembered by its exact program, arguments and working directory, a write or
  edit by its file, and an MCP call by its tool. It used to approve the whole
  tool, so approving one `ls` allowed every later command. The option's label
  now says what it covers.
- A session whose agent process died while a tool ran takes prompts again.
  The stored history ended with a tool call that had no result, and the
  DeepSeek API rejected every later request with HTTP 400. Loading such a
  session now records the call as interrupted, and each model request answers
  any tool call that still has no result.
- An ACP v1 prompt response reports the usage of that turn alone, as ACP
  defines it, instead of the session's running total, and declares it with
  `_meta["mjolnir.dev/usage-scope"] = "turn"`. A local command reports zero.
  `usage_update` still carries the session total.
- A rejected API key names `DEEPSEEK_API_KEY`, and a failed connection gives its
  cause (such as "Connection refused") and names `DEEPSEEK_BASE_URL`.

## 0.2.0 - 2026-10-22

### Added

- npm package `@brokkai/dsh-acp`, which bundles the native binaries for
  macOS, Linux (glibc 2.34 or newer), and Windows on x64 and arm64 behind a
  small Node.js launcher, with no npm dependencies or install scripts.
- Release workflow that builds and tests all six platforms, publishes verified
  archives with SHA-256 checksums and `release.json` metadata to GitHub
  Releases, then publishes to npm through trusted publishing with provenance.

### Changed

- Raised the minimum supported Rust version to 1.89.
- Updated all dependencies to their latest releases, including `reqwest` 0.13,
  `base64` 0.23, `tokio` 1.54, `uuid` 1.28, and `schemars` 1.2.3.
- HTTPS now verifies certificates against the operating system's trust store
  (`reqwest` 0.13 `rustls` with the platform verifier) instead of bundled
  WebPKI roots.

## 0.1.0 - 2026-10-20

### Added

- Dual-stack Agent Client Protocol support through `agent-client-protocol` 3.4,
  with ACP v2 negotiation and v1 fallback.
- DeepSeek OpenAI-compatible streaming for text, reasoning, images, tool calls,
  provider routing, and token usage.
- Built-in workspace file, search, edit, command, web, plan, and user-question
  tools with ACP permission and elicitation integration.
- MCP stdio and streamable HTTP clients, tool discovery refresh, structured
  result forwarding, and model-qualified tool names.
- Durable session storage, list, resume, replay, fork, delete, and close
  operations.
- ACP v2 asynchronous prompt acceptance, foreground-state reporting, replay
  cursors, structured diffs, agent-owned terminal snapshots and output chunks,
  plans, compactions, notices, and provider configuration.
- ACP v1 filesystem and terminal delegation when the client advertises support.
- The ecosystem `_session/steering` extension on v1 and v2 with
  `idleBehavior: "promptRequired"`.
- `deepseek-api-key` authentication descriptors and login/logout handlers.
- Form and URL elicitation support when advertised by the client.
- Integration coverage for v1/v2 handshakes, images, tools, failed-tool replay,
  terminal output, commands, cancellation, and steering.

[Unreleased]: https://github.com/BrokkAi/dsh-acp/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.2.3...v0.3.0
[0.2.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.1.0...v0.2.0
