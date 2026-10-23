# Changelog

All notable changes to `dsh-acp` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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

[Unreleased]: https://github.com/BrokkAi/dsh-acp/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/BrokkAi/dsh-acp/compare/v0.1.0...v0.2.0
