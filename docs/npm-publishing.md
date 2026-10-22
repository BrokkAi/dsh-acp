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

## First publication and trusted publisher setup

The package must exist before configuring its trusted publisher, so the first
npm version is published by hand. Use an npm account with publishing access to
the `@brokkai` scope and 2FA enabled. `npm trust` is built into the npm CLI; no
Cargo plugin is required.

`v0.1.0` predates the release workflow and has no complete set of platform
archives, so the first npm version is the first release that `release.yml`
publishes. That tag run publishes the GitHub release, then its npm step fails
because the package does not exist yet. From a clean checkout at that tag, with
its verified release assets in `dist/`, run:

```sh
gh release download vX.Y.Z --repo BrokkAi/dsh-acp --dir dist --pattern 'dsh-acp-*'
npm login --registry=https://registry.npmjs.org/
python3 scripts/npm_release.py pack
npm publish ./npm-dist/brokkai-dsh-acp-X.Y.Z.tgz --access public --ignore-scripts
npm trust github @brokkai/dsh-acp --repo BrokkAi/dsh-acp --file release.yml --allow-publish
npm trust list @brokkai/dsh-acp --json
```

Replace `X.Y.Z` with the Cargo version. Complete npm's browser/2FA prompts
when requested. The trusted publisher must use owner `BrokkAi`, repository
`dsh-acp`, and workflow filename `release.yml`, with direct publishing allowed
and no environment restriction (the workflow does not declare an environment).

Then rerun the tag workflow's failed publisher job. It verifies the completed
GitHub release read-only and finds the npm version already published; it
passes only if the registry tarball is identical to the one it packs, and
otherwise fails without publishing. The next release tag is the end-to-end
verification that the trust configuration works.

The packager rejects missing platforms, corrupt archives, and release metadata
that does not match the checked-out commit and Cargo version. It accepts the
`npm pack` output of both npm 11 and npm 12.

See npm's [trusted publishing documentation](https://docs.npmjs.com/trusted-publishers/)
and [`npm trust` reference](https://docs.npmjs.com/cli/v12/commands/npm-trust/).
