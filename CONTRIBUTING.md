# Contributing

Thank you for helping improve `dsh-acp`.

## Development setup

Install Rust 1.89 or newer and provide a DeepSeek-compatible endpoint for
manual testing. Automated tests use a loopback mock provider and do not need an
API key.

Before submitting a pull request, run:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
```

## Pull requests

- Keep changes focused and explain externally visible behavior.
- Add regression coverage for protocol lifecycle, replay, tool, permission,
  cancellation, security, or concurrency changes.
- Update the README and relevant files under `docs/` when compatibility or
  configuration changes.
- Preserve ACP v1 and v2 semantics separately. A successful v2 negotiation
  does not enable an unstable feature by itself.
- Never commit credentials, session transcripts, private logs, customer data,
  or local environment files. Redact diagnostics before attaching them.
- Report security issues according to [SECURITY.md](SECURITY.md), not in a
  public issue.

Unless explicitly stated otherwise, contributions intentionally submitted for
inclusion are licensed under the MIT License.

## Protocol references

Use the current [ACP specification](https://agentclientprotocol.com/) and the
in-repository [support matrix](docs/acp-support.md). Do not infer peer support
from local SDK feature flags; follow the capability and lifecycle rules for the
selected protocol version.
