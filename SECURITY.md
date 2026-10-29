# Security Policy

## Supported versions

Use the latest published release. Security fixes are developed on `master` and
delivered in a new release when necessary.

## Reporting a vulnerability

Do not disclose vulnerabilities, credentials, session transcripts, private
repository contents, or working exploits in a public issue.

Use GitHub's **Report a vulnerability** action on the repository Security page.
If that action is unavailable, contact a repository maintainer through a
private channel listed on their GitHub profile and ask them to open a draft
security advisory.

Include the affected version or commit, impact, reproduction steps, and any
suggested mitigation. Remove credentials and unrelated personal data from logs.

Security-sensitive areas include workspace path confinement, command execution,
MCP server launch configuration, provider headers, permission policy, terminal
output handling, persisted session files, and the `--listen` HTTP and WebSocket
server: its bearer token, `Host` and `Origin` checks, loopback-only default,
and one-connection-at-a-time rule.
