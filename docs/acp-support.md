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
- Unsupported versions are rejected through the SDK protocol router.

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
| Provider configuration | Implemented | Advertised provider capability |
| MCP stdio | Implemented | Advertised transport |
| MCP streamable HTTP | Implemented | Advertised transport |
| Form elicitation | Implemented | Client capability |
| URL elicitation | Implemented | Client capability |
| `_session/steering` | Implemented | `_meta.steering.supported` |
| Steering idle return | Implemented | `idleBehavior: "promptRequired"` |
| Agent-owned terminals | Implemented in v2 | Baseline v2 |
| Terminal output chunks | Implemented in v2 | Baseline v2 |
| Tool content chunks | Implemented in v2 | Baseline v2 |
| Plans | Implemented | Baseline |
| Compaction | Implemented | Baseline v2; negotiated in v1 |
| Notices | Implemented for MCP refresh failures | v2 baseline; negotiated in v1 |
| Subagents | Not implemented | Draft |
| MCP-over-ACP | Not implemented | Draft |
| ACP Streamable HTTP transport | Not implemented | Draft |

Audio is intentionally not advertised because the DeepSeek input API used by
this harness does not accept audio content.

## Session configuration options

Both protocol versions expose select options for `model`, `mode`,
`permission_mode`, and `reasoning_effort`, plus a boolean `network` toggle.
`model`, `mode`, and `reasoning_effort` carry the ACP `model`, `mode`, and
`thought_level` categories; `permission_mode` is uncategorized. The thinking
effort carries `none`, `low`, `high`, or `max`, is stored with the session, and
is sent to DeepSeek as `reasoning_effort`. `temperature` is unaffected by
effort and rides every request except the legacy `deepseek-reasoner` model.

## Prompt lifecycle

ACP v2 responds to `session/prompt` after inserting the user message, before
model work completes. Foreground state is reported through `state_update` and
ends with an idle snapshot and stop reason.

ACP v1 retains its request-scoped prompt response: the response is sent when
the turn completes and carries the stop reason directly.

Both versions persist user content, assistant output, tool calls, outcomes,
plans, compactions, and terminal/diff state for replay.

## Permissions and workspace boundaries

Reads, writes, searches, local commands, and MCP calls execute in the agent.
The session `cwd` and additional roots define the filesystem boundary. Sensitive
tools request permission unless the session policy or an explicit remembered
decision allows them.

In plan mode, read-only MCP tools marked with `annotations.readOnlyHint: true`
remain available and may be called. Other MCP tools are hidden and refused
before permission is requested.

A write or edit shows its change before it runs: the pending tool call and the
permission request carry a diff of the whole file (old and new text in v1, a
patch in v2). When the change cannot be worked out, for example because the
text to replace is not in the file, they carry no diff and the tool reports
the error when it runs.

ACP v1 may delegate text file access and terminal execution to a capable client.
A command that runs in the client's terminal puts that terminal in its tool
call as soon as the client creates it. The final update keeps the terminal and
adds the result text only if the command failed; replay after `session/load`
shows the result text, because the terminal has been released.
ACP v2 has no client filesystem or terminal execution methods; use an MCP server
for client-provided tools.
