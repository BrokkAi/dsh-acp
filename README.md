# dsh-acp

**A full-featured Agent Client Protocol server for DeepSeek Harness.**

[Agent Client Protocol](https://agentclientprotocol.com/) clients use
`dsh-acp` to drive real [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)
sessions from editors and other tools.

```text
ACP client
  -> dsh-acp Rust ACP v1/v2 server
  -> pinned official DeepSeek Harness runtime
  -> DSH model loop, tools, permissions, sessions, and plugins
```

`dsh-acp` owns the complete editor-facing ACP surface. DeepSeek Harness owns
execution: model calls, the agent loop, tool execution, sandbox policy,
permissions, MCP clients, and session persistence.

> This is an independent community project. It is not affiliated with,
> endorsed by, or supported by DeepSeek.

## Supported runtime

The runtime is pinned to the exact official release:

```text
@deepseek-ai/dsh 0.2.2-rc.5
```

The npm package installs that release and resolves its runtime modules for the
native bridge. A source build can use an installed `dsh` executable or
`--dsh-js`. In every case the bridge verifies the exact DSH version before a
session starts. A missing or mismatched runtime fails loudly.

The bridge extends DSH's SDK runtime rather than proxying DSH's
automation-only `--profile acp` surface. The embedded runtime plugin exposes
the missing controls the Rust ACP server needs:

- explicit session open, close, list, resume, and delete;
- model, reasoning-effort, permission, and mode changes;
- cancellation and steering;
- live assistant-stream frames;
- DSH command discovery;
- per-session MCP client mounting;
- session snapshots and titles.

The official DSH SDK runtime still owns prompts, approvals, user questions,
durable events, model requests, and tool execution.

## ACP features

- ACP v1 and v2 negotiation.
- Async ACP v2 prompt acceptance and foreground state.
- Streamed assistant text and reasoning from DSH model calls.
- DSH tool-call and tool-result lifecycle updates.
- ACP permission requests backed by DSH approvals.
- Plan updates from DSH todo state.
- Model and reasoning-effort selection.
- Agent and plan modes backed by DSH's plan-mode service.
- DSH commands such as `/compact`, `/permission`, `/plan`, `/goal`, and
  `/feedback`.
- Session new/list/load/resume/fork/delete/close and replay.
- DSH session persistence and MCP stdio/Streamable HTTP servers.
- Text and image prompts supported by the selected DSH model.
- stdio, and opt-in authenticated streamable HTTP/WebSocket.

See [ACP support](docs/acp-support.md) for the exact surface and limitations.

## Install

The npm package requires Node.js 24.9 or newer and bundles the native bridge
for macOS, Linux, and Windows on x64 and arm64:

```bash
npm install -g @brokkai/dsh-acp
dsh-acp --version
```

Or run it without a global install:

```bash
npx --yes @brokkai/dsh-acp
```

Linux binaries require glibc 2.34 or newer. musl systems such as Alpine are not
supported.

> The package is the scoped `@brokkai/dsh-acp`. The unscoped `dsh-acp` package
> is a different project.

To build from source, install Rust 1.89 or newer and the pinned DSH release:

```bash
npm install -g @deepseek-ai/dsh@0.2.2-rc.5
cargo install --git https://github.com/BrokkAi/dsh-acp
```

## Use from an ACP client

Set the provider credential accepted by DSH, normally `DEEPSEEK_API_KEY`, and
launch `dsh-acp` as the agent command:

```json
{
  "command": "dsh-acp",
  "args": [],
  "env": {
    "DEEPSEEK_API_KEY": "..."
  }
}
```

Zed example:

```json
{
  "agent_servers": {
    "DeepSeek Harness": {
      "command": "dsh-acp",
      "env": {
        "DEEPSEEK_API_KEY": "..."
      }
    }
  }
}
```

ACP traffic uses stdout. Diagnostics, tracing, and DSH stderr use stderr.

## Configuration

The existing agent-facing options remain:

```text
--api-key-file <PATH>            Read the DeepSeek API key from a protected file
--base-url <URL>                 DeepSeek-compatible Messages API root
--model <MODEL>                  Default model
--models <A,B>                   Models in the ACP selector
--data-dir <PATH>                ACP session projection (default: .dsh-acp)
--permission-mode ask|allow|deny Sensitive-tool policy
--system-prompt <TEXT>           Extra system instructions
--reasoning-effort <EFFORT>      Thinking effort
--disable-web                    Disable web access
--trace                          Detailed diagnostics on stderr
--listen <ADDR>                  Serve HTTP/WebSocket instead of stdio
--listen-allow-remote            Permit non-loopback listening
```

DSH runtime options:

```text
--dsh-command <PATH>             Exact dsh executable
--dsh-profile <NAME>             DSH profile (default: sdk)
--dsh-home <PATH>                Explicit DSH_HOME
```

The npm launcher supplies the pinned DSH JavaScript entry point and module
root automatically. `DSH_ACP_DSH_COMMAND` and `DSH_ACP_DSH_JS` can override the
launcher for development, but the reported version must still be
`0.2.2-rc.5`.

## Session storage

DSH remains authoritative for model history, tool execution, and persisted
session logs. The Rust server stores the ACP projection needed for replay,
titles, options, and compatibility metadata.

Use an isolated DSH home when needed:

```bash
dsh-acp --dsh-home /absolute/path/to/dsh-home
```

## HTTP and WebSocket

`--listen` serves ACP at `/acp` with the SDK's streamable HTTP and WebSocket
transport. It requires a bearer token, binds loopback by default, rejects
browser `Origin` requests, validates `Host`, and serves one ACP connection at
a time.

The transport RFD is still unstable. See the previous security behavior in
[Architecture](docs/architecture.md).

## Testing

No-key tests use a local fake Messages API:

```bash
cargo test
node --test npm/test/launcher.test.cjs
python3 -m unittest discover -s scripts -p 'test_*.py'
node scripts/smoke_npm.cjs target/debug/dsh-acp
```

Real-DSH integration tests require the pinned runtime:

```bash
DSH_ACP_REAL_DSH_JS=/path/to/node_modules/@deepseek-ai/dsh/lib/bin.js \
  cargo test --test dsh_acp --test dsh_runtime -- --ignored
```

They exercise v1 and v2 prompts, streaming, cancellation, permission-gated
writes, session persistence, list/resume/close, DSH version verification, and
clean shutdown.

## License

MIT
