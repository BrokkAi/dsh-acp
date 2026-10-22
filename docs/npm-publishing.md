# Publishing to npm

The public package is `@brokkai/dsh-acp`; its executable is `dsh-acp`.
`npm/package.json` is a private packaging template. The packager takes the
version from `Cargo.toml`, removes `private`, and bundles the six verified
native release binaries with a small Node.js launcher. It includes no npm
dependencies or install scripts. Do not publish the template directory.

## Release workflow

`.github/workflows/release.yml` builds and tests the native binary and npm
launcher on every supported platform. Its publisher job verifies all release
archives, packs and installs the npm tarball for a smoke test, and publishes
on an explicit `v*` tag push. GitHub Releases is published before npm. Branch
pushes and manual workflow dispatches only validate the npm package.

The publisher uses Node.js 24, npm 12.2.1, and `id-token: write`. npm obtains
short-lived publishing credentials through GitHub OIDC; no npm token secret
is needed. Trusted publishing also generates npm provenance for the public
repository. A rerun skips an existing npm version only if its tarball integrity
matches exactly; different contents require a new version. Prereleases use
the `next` dist-tag. After publishing, verification waits up to five minutes
for npm's registry metadata to become available without publishing again.

## Initial package bootstrap and trusted publisher setup

npm requires a package to exist before a trusted publisher can be configured.
The package name is reserved by a metadata-only `0.0.1` bootstrap release under
the `bootstrap` dist-tag. npm assigns the first package version to `latest`
even when another tag is requested, so `0.0.1` is deprecated and must be
replaced as `latest` by the first substantive release. The bootstrap package
contains no executable and must not be used.

Trusted publishing is configured for owner `BrokkAi`, repository `dsh-acp`,
and workflow filename `release.yml`, with direct publishing allowed and no
environment restriction (the workflow does not declare an environment). The
first substantive release, `0.2.0`, exercises this configuration end to end.

`npm trust` is built into the npm CLI; no Cargo plugin is required. The
configuration can be inspected with:

```sh
npm trust list @brokkai/dsh-acp --json
```

If npm publication fails after GitHub succeeds, rerun the tag workflow's
publisher job. It verifies the completed GitHub release read-only and either
publishes a missing npm version or finds the published version; it passes only
if the registry tarball is identical to the one it packs, and otherwise fails
without publishing.

The packager rejects missing platforms, corrupt archives, and release metadata
that does not match the checked-out commit and Cargo version. It accepts the
`npm pack` output of both npm 11 and npm 12.

See npm's [trusted publishing documentation](https://docs.npmjs.com/trusted-publishers/)
and [`npm trust` reference](https://docs.npmjs.com/cli/v12/commands/npm-trust/).
