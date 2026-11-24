# Architecture

## Process model

`dsh-acp` is a Rust ACP server and DSH runtime supervisor.

```text
ACP client
  |
  | ACP v1 or v2
  v
Rust frontend
  |
  | private JSON-RPC over child stdio
  v
pinned official DSH SDK profile + dsh-acp runtime plugin
  |
  +-- DSH model adapters and HTTP calls
  +-- DSH agent loop and tools
  +-- DSH permissions and user questions
  +-- DSH sessions and persistence
  +-- DSH MCP clients
  +-- DSH plugins
```

The Rust process owns protocol negotiation, ACP method semantics, replay,
client interactions, and transport security. DSH owns execution.

This deliberately does not proxy `dsh --profile acp`; that profile is a
separate automation-only ACP frontend and does not expose the full editor
surface.

## Runtime startup

Each ACP connection has a lazy DSH runtime provider.

1. The frontend writes its embedded runtime plugin to `<data-dir>/dsh-runtime/`.
2. It starts DSH with the `sdk` profile and a generated patch that disables the
   stock SDK protocol server and inserts the `dsh-acp` runtime plugin.
3. It sets the pinned runtime's module root, DSH home, credential environment,
   and API base URL.
4. It performs `dsh/hello` and verifies the exact `0.2.2-rc.5` version.

DSH is started only when the first session/list/new/resume or prompt needs it.
ACP `initialize` itself remains cheap.

## Runtime plugin

The embedded plugin extends `HarnessSdkJsonRpcServer` instead of
reimplementing DSH behavior. The stock server retains:

- session prompt admission;
- model execution and retries;
- approval request dispatch;
- user-question dispatch;
- durable session-event publication;
- agent status;
- subagent lifecycle.

The plugin adds:

- `dsh/session/open`
- `dsh/session/close`
- `dsh/session/delete`
- `dsh/session/list`
- `dsh/session/resume`
- `dsh/session/cancel`
- `dsh/session/steer`
- `dsh/session/set_config`
- `dsh/session/set_mode`
- `dsh/session/mcp`
- `dsh/session/commands`
- `dsh/session/set_title`
- `dsh/session/snapshot`
- `dsh/assistant-stream` notifications

This keeps the DSH-specific layer narrow and uses official runtime ownership
for execution.

## Prompt flow

### ACP v1

1. The client sends `session/prompt`.
2. Rust validates and records the prompt.
3. Rust sends `session/query` to DSH.
4. DSH streams assistant frames and durable session events.
5. Rust maps raw frames to ACP message/thought chunks and durable events to
   tool, plan, title, usage, and completion updates.
6. DSH receives reverse `approval/request` and `user-question/ask` calls; Rust
   maps them to ACP requests.
7. The prompt settles from DSH's turn end and `session/wait`.

### ACP v2

ACP v2 `session/prompt` returns its message ID immediately. A detached Rust
task drives the same DSH query, then emits v2 `running` and `idle` state.
`session/cancel` cancels both the Rust token and the exact DSH session.

## Sessions and persistence

An ACP session ID is the DSH session ID. DSH persistence remains authoritative
for execution history. The Rust projection stores:

- original ACP prompt blocks;
- observed DSH events;
- replayable assistant/tool projections;
- titles, options, compactions, and usage needed by ACP v1/v2.

Session list is read from DSH persistence. Resume validates the stored cwd and
asks DSH to restore the exact session. Close drains and disposes the DSH agent.
Delete closes any live agent and removes the DSH persistence artifact.

## MCP

ACP session MCP declarations are sent to the runtime plugin. The plugin mounts
them in the exact Agent scope using the official DSH MCP client. Tool discovery
takes place before the session is used, and replies flow through DSH's normal
tool-event path.

## Concurrency

- One ACP connection is served at a time over `--listen`.
- Each connection gets its own DSH runtime process and in-memory Harness.
- A session has one active foreground prompt.
- Steering serializes through the session turn lock and is forwarded to DSH.
- Runtime events are filtered by exact session ID before projection.
- Reverse DSH requests are answered through the ACP connection that owns the
  turn.

## Shutdown

Closing a session disposes the DSH-owned Agent and flushes persistence. Ending
an ACP connection drops its runtime provider; the child is started with
`kill_on_drop`, and normal session close/flush completes before process exit.

## Security

DSH applies workspace confinement, command policy, approvals, credential
resolution, MCP transport policy, and tool output limits. The Rust frontend
retains:

- ACP workspace-root validation;
- non-loopback HTTP/WebSocket refusal by default;
- bearer-token and Host/Origin checks;
- structured prompt and system-prompt validation;
- session replay bounds;
- exact DSH version verification.

Provider credentials are passed through the child environment and are removed
from the parent environment before runtime threads start.
