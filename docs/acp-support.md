# ACP support

This is the compatibility contract for the Rust ACP frontend running against
the pinned official DSH runtime.

## Version negotiation

- ACP v1 and v2 are supported.
- ACP v2 uses asynchronous `session/prompt` acceptance and `state_update`
  running/idle transitions.
- Unsupported protocol versions follow the Rust SDK router's negotiation and
  error behavior.

## Agent methods

| Method | ACP v1 | ACP v2 | Owner |
|---|---|---|---|
| `initialize` | Yes | Yes | Rust frontend |
| `authenticate` | Yes | Yes | Rust frontend + DSH credential |
| `session/new` | Yes | Yes | Rust projection + DSH session |
| `session/load` | Yes | — | Rust replay + DSH resume |
| `session/resume` | Yes | Yes | Rust projection + DSH resume |
| `session/fork` | Yes | Yes | ACP session/Rust projection |
| `session/list` | Yes | Yes | DSH persistence |
| `session/delete` | Yes | Yes | DSH persistence + ACP projection |
| `session/close` | Yes | Yes | DSH agent disposal |
| `session/prompt` | Yes | Async | DSH SDK runtime |
| `session/cancel` | Yes | Yes | DSH agent cancellation |
| `session/set_mode` | Yes | Config option | DSH plan-mode service |
| `session/set_config_option` | Yes | Yes | DSH model/permission state |
| `session/set_title` | Yes | Yes | DSH title service + ACP projection |
| `session/status` | Yes | Yes | Rust connection state |
| `session/options` | Yes | Yes | ACP projection |

## Updates

- Assistant text and reasoning stream live from DSH's
  `agent/assistant-stream`.
- Tool calls and results come from DSH session events.
- Todo replacement events become ACP plans.
- DSH title events become `session_info_update`.
- DSH usage events become ACP usage updates.
- DSH turn endings settle v1 prompts and v2 idle state.
- MCP tool failures remain failures of the individual tool call.

## Permissions

DSH policy decides whether an operation needs approval. The runtime plugin
forwards the approval as a reverse request. Rust presents the exact DSH tool
call with one-shot ACP options:

- allow once;
- reject.

Client cancellation is forwarded as cancellation. The write, command, or MCP
operation itself executes only inside DSH.

## MCP

The server accepts ACP stdio and Streamable HTTP MCP declarations. The runtime
plugin mounts each server in the DSH Agent scope and lets DSH discover and call
its tools. Duplicate or invalid declarations fail session setup.

MCP-over-ACP is not currently advertised because the pinned DSH MCP client
accepts stdio and Streamable HTTP transports only.

## Content

- Text and resource-link prompts are supported.
- Raster image prompts are advertised only when the current DSH model route can
  accept images.
- Embedded text resources are supported.
- Audio is not advertised.
- ACP client-owned terminals and filesystem reads are not used for DSH's own
  coding tools; DSH executes those tools inside its configured sandbox.

## Sessions and persistence

DSH owns the durable execution log and model-visible history. Rust owns the ACP
projection required for v1 replay and v2 entity updates. Replay reads the Rust
projection; resume restores the same session ID in DSH and validates the cwd.

## Steering

`_session/steering` remains available on both ACP versions. An active steering
request is sent to DSH's Agent inbox. The `promptRequired` idle behavior is
preserved for ACP v1.

## Known limitations

- DSH currently exposes one workspace root per session. `additionalDirectories`
  is rejected and not advertised.
- Provider switching is accepted by the ACP compatibility surface, but deep
  runtime provider reconfiguration remains limited by what the pinned DSH SDK
  profile exposes.
- Prompt suggestions are disabled on the DSH execution path because the pinned
  runtime has no native suggestion service.
- DSH tool events expose generic lifecycle data and text content. Rich
  DSH-specific card metadata is not part of the public DSH session event.
- The pinned runtime remains a developer preview; upgrades require matching
  runtime/plugin changes and the real-DSH integration suite.
