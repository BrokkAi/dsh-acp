# dsh-acp

`dsh-acp` is a DeepSeek-powered coding agent that speaks the
[Agent Client Protocol](https://agentclientprotocol.com/) over stdio. It is
built on `agent-client-protocol` 3.4, negotiates ACP v2 when the client
requests it, and falls back to ACP v1 for older clients.

It is not a chat-only adapter. The agent can stream reasoning and responses,
call filesystem and terminal operations through the ACP client, run local
fallback tools, use permission prompts, consume MCP servers, persist and replay
sessions, switch modes and models, and report token usage.

## Build

```bash
cargo build --release
```

The binary is `target/release/dsh-acp`.

## Run

ACP agents communicate on stdout, so diagnostics are written to stderr.

```bash
export DEEPSEEK_API_KEY="..."
./target/release/dsh-acp
```

Typical client configuration:

```json
{
  "command": "/absolute/path/to/dsh-acp",
  "args": [],
  "env": {
    "DEEPSEEK_API_KEY": "..."
  }
}
```

Useful options:

```text
--base-url <URL>                 OpenAI-compatible API root
--model <MODEL>                  Default model (default: deepseek-flash)
--models <A,B>                   Models in the ACP model selector
--data-dir <PATH>                Session storage (default: .dsh-acp)
--permission-mode ask|allow|deny Sensitive-tool policy
--max-tool-rounds <N>            Model/tool round limit
--context-chars <N>              Approximate context budget
--tool-output-bytes <N>          Per-tool output cap
--disable-web                    Disable web_fetch
--trace                          Detailed diagnostics on stderr
```

Every option has an environment fallback; run `dsh-acp --help` for the complete
list.

## ACP support

The server implements both protocol generations through the SDK protocol
router. ACP v2 is primary and v1 is the compatibility path.

ACP v2 includes asynchronous prompt acceptance, foreground `running` /
`requires_action` / `idle` state, patch-style tool calls, v2 replay cursors,
v2 config options, permissions, elicitation, session fork/resume/delete/close,
active-session resume, agent-owned terminal snapshots, structured file diffs,
context-compaction updates, MCP transport declarations, and provider
configuration.

Both protocol versions advertise the ecosystem `_session/steering` extension
through top-level `_meta.steering.supported`, and both accept the
`idleBehaviors: ["promptRequired"]` opt-in. Steering injects a prompt into the
active model loop, and the loop consumes it at the next safe model boundary
before reporting `idle`. An idle steering request with its default behavior
starts a new detached turn; with `idleBehavior: "promptRequired"`, it returns
`promptRequired` so the Client can retain and submit the prompt itself.

ACP v1 includes:

- `initialize`, agent info, capabilities, auth/logout, and optional provider
  configuration APIs
- `session/new`, `session/load`, `session/resume`, `session/fork`,
  `session/list`, `session/delete`, and `session/close`
- `session/prompt`, `session/cancel`, `session/set_mode`, and
  `session/set_config_option`
- `session/update` streaming for agent messages, thoughts, tool calls, tool
  progress/results, modes, config, session info, and token usage
- Agent-to-client permission, filesystem, and terminal requests
- MCP stdio and streamable HTTP server declarations and tool calls
- Session persistence, pagination, replay, forking, system prompts, additional
  workspace roots, and end-of-turn token usage

Both protocol versions advertise the `deepseek-api-key` login method and expose
their corresponding authentication and logout requests. Credentials still come
from the launch environment, CLI, or provider configuration; the protocol login
method re-enables a configured provider rather than carrying a secret inline.

Legacy SSE-only MCP transports are best effort and are not advertised.
MCP-over-ACP requires a proxy component and is not advertised by this
standalone agent.

## Tools

Built-in model tools:

| Tool | Purpose |
| --- | --- |
| `update_plan` | Publish a complete ACP-visible execution plan |
| `ask_user` | Request structured input through ACP elicitation |
| `read_file` | Read text with line ranges |
| `list_directory` | Bounded recursive directory listing |
| `search_files` | Regex content search with path filtering |
| `write_file` | Create or overwrite files |
| `edit_file` | Exact-text replacement with ambiguity checks |
| `run_command` | Execute a command without a shell |
| `web_fetch` | Fetch and extract bounded readable web content |

File paths are restricted to the session working directory and additional
workspace roots. Symlink escapes are rejected. Writes, commands, and MCP tools
go through ACP permission requests unless the session policy or a prior
"allow always" decision permits them. `web_fetch` rejects localhost, private,
link-local, and multicast destinations for both direct requests and redirects.

In v1, when the ACP client advertises filesystem and terminal capabilities,
the corresponding reverse requests are used. Local implementations are
available as fallbacks. ACP v2 no longer exposes those client capabilities, so
v2 uses the local workspace tools directly.

MCP tools are exposed to the model as
`mcp__<server>__<tool>`. Both stdio servers and modern streamable HTTP servers
are supported; a session can declare multiple servers.

## DeepSeek behavior

Responses use DeepSeek's OpenAI-compatible streaming API. Reasoning chunks are
forwarded as ACP thought updates. Tool calls are assembled from streamed
fragments, executed sequentially, returned as tool messages, and fed back to
the model until it finishes or the configured round limit is reached.

`deepseek-flash` supports JPEG, PNG, GIF, and WebP image inputs. ACP image
content is translated into DeepSeek/OpenAI-compatible `image_url` parts and is
retained in session history for replay.

`ask_user` uses ACP form elicitation by default. When the Client advertises URL
elicitation, the model may also request an HTTPS URL with an optional stable
`elicitation_id`. Embedded URL credentials and plain HTTP outside localhost are
rejected.

Session model and permission configuration is available through ACP config
options. The in-conversation commands `/help`, `/plan`, `/agent`, `/model`, and
`/status` are also supported.

## Persistence

Sessions are stored as readable JSON under:

```text
<data-dir>/sessions/<session-id>.json
```

Writes are atomic. The transcript includes text, reasoning, original prompt
content blocks, tool calls, structured tool outcomes and raw outputs, diffs,
terminal snapshots, plans, compactions, configuration, timestamps,
parent-session links, cumulative token usage, and current context occupancy.
Replay restores failed and cancelled tool statuses, rendered diffs and patches,
terminal state, plans, compactions, and image/resource prompt content.

## Verification

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
```

The integration suite performs real ACP v1 and v2 stdio handshakes and runs
mocked DeepSeek streams through complete tool round trips, permissions, and
cancellation. Binding its loopback mock server may require network permission
in heavily sandboxed environments.

