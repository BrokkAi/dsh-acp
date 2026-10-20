# Changelog

All notable changes to `dsh-acp` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[Unreleased]: https://github.com/BrokkAi/dsh-acp/compare/v0.1.0...HEAD
