# Steering

`dsh-acp` implements the ecosystem `_session/steering` extension used by Codex
and Claude ACP adapters. It is an extension method rather than a stable core ACP
method.

## Advertisement

Both initialize responses include:

```json
{
  "_meta": {
    "steering": {
      "supported": true,
      "idleBehaviors": ["promptRequired"]
    }
  }
}
```

## Request

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "method": "_session/steering",
  "params": {
    "sessionId": "dsh-...",
    "prompt": [
      { "type": "text", "text": "Use the safer approach." }
    ],
    "_meta": {
      "steering": {
        "idleBehavior": "promptRequired"
      }
    }
  }
}
```

The prompt is a non-empty ACP content-block array. Image prompts follow the same
model capability checks as `session/prompt`.

## Outcomes

| Outcome | Meaning |
| --- | --- |
| `injected` | The input joined the active turn. |
| `startedNewTurn` | No turn was active; a detached turn was started. |
| `promptRequired` | No turn was active and the client asked to retain the input. |
| `failed` | A new turn was accepted but could not be started. |

For `promptRequired`, the response includes:

```json
{
  "outcome": "promptRequired",
  "reason": "noRunningTurn"
}
```

Any `idleBehavior` other than `promptRequired` is rejected with `-32602`.

## Delivery semantics

Steered input is acknowledged with a normal user-message update and retained in
session history. Multiple steering requests for a session are serialized.

Active steering is forwarded to the exact DSH Agent inbox. DeepSeek Harness
consumes it at its next safe step boundary; it does not silently start a second
concurrent model request. The Rust frontend still owns ACP acknowledgment,
`promptRequired`, and the session turn lock.
