# Changelog

All notable changes to `dsh-acp` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- An `initialize` request with an unsupported or malformed `protocolVersion`
  now always gets its error response (`-32022` with the requested and
  supported versions, or `-32602`) before the server exits. The response was
  often lost, most often while the client still had stdin open. The same fix
  delivers the `-32600` error for a first request that is not `initialize`, and
  the `-32700` error for a first line that is not JSON.

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

[Unreleased]: https://github.com/BrokkAi/dsh-acp/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.2.3...v0.3.0
[0.2.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.1.0...v0.2.0
