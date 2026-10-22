# Contributing

Thank you for helping improve `dsh-acp`.

## Development setup

Install Rust 1.89 or newer, Python 3.11 or newer, and Node.js 22 or newer, and
provide a DeepSeek-compatible endpoint for manual testing. Automated tests use
a loopback mock provider and do not need an API key.

Before submitting a pull request, run:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
node --test npm/test/launcher.test.cjs
python3 -m unittest discover -s scripts -p 'test_*.py'
node scripts/smoke_npm.cjs target/debug/dsh-acp
```

The script tests check that the npm launcher, `scripts/release.py`, and the
release workflow list the same targets. The smoke test runs the native binary
through the npm launcher, including an ACP `initialize` handshake. The release
workflow runs the Rust tests, launcher tests, and smoke test on all six release
platforms.

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

## Maintainer releases

See [RELEASING.md](RELEASING.md) for preparation, exact-commit checks,
publication, and recovery, and [npm publishing](docs/npm-publishing.md) for the
npm package. Preflight branch pushes and manual dispatches do not publish.

## Protocol references

Use the current [ACP specification](https://agentclientprotocol.com/) and the
in-repository [support matrix](docs/acp-support.md). Do not infer peer support
from local SDK feature flags; follow the capability and lifecycle rules for the
selected protocol version.
