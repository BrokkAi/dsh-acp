# Releasing dsh-acp

Releases publish to `BrokkAi/dsh-acp` GitHub Releases and to npm as
`@brokkai/dsh-acp`. The npm package bundles all six native binaries and a
Node.js launcher. The same tag workflow publishes GitHub assets first, then
npm. See [npm publishing](docs/npm-publishing.md) for trusted publisher setup,
package verification, and recovery rules. Source installation remains
available through `cargo install --git`.

Each `v<version>` release has twelve assets: an archive and `.sha256` sidecar
for each of:

- `x86_64-unknown-linux-gnu` (tar.gz)
- `aarch64-unknown-linux-gnu` (tar.gz)
- `x86_64-apple-darwin` (tar.gz)
- `aarch64-apple-darwin` (tar.gz)
- `x86_64-pc-windows-msvc` (zip)
- `aarch64-pc-windows-msvc` (zip)

Names are `dsh-acp-v<version>-<target>.<format>`. Each archive includes the
binary, README, LICENSE, and `release.json` with the exact commit, version,
platform, file hashes and permissions. Windows archives have flat paths; Unix
archives keep a versioned parent directory. Packaging runs the built binary
and requires `dsh-acp --version` to report the Cargo version.

Linux binaries are built on Ubuntu 22.04 runners so they need at most glibc
2.34 (Ubuntu 22.04, Debian 12, RHEL 9). Packaging reads the binary's strong
glibc symbol versions with `objdump -T` and refuses a Linux binary that needs
anything newer, so a runner or dependency change cannot raise the minimum
silently.

A version with a pre-release suffix, such as `0.2.0-rc.1`, publishes as a
GitHub prerelease that does not become the latest release, and to the npm
`next` dist-tag.

## Preparation (does not publish)

1. Start from an up-to-date `master` on a `brb/release-*` topic branch.
   Increment the version in `Cargo.toml` and `Cargo.lock` together and move
   the `Unreleased` entries in `CHANGELOG.md` under the new version. Never
   reuse a completed release version.
2. Run the contributing checks. Push the topic branch and open a PR to master.
   `ci.yml` runs on PRs and on master, release-topic, and tag pushes.
   `release.yml` runs on master and release-topic pushes; these runs build and
   test all six platforms and validate the publisher without publishing. After
   a release, master keeps the released version until the next bump; its
   `release` runs still build and test every platform, but the publisher
   preflight passes as a no-op instead of failing on the existing tag, so a
   red `release` run on master is always a real failure. A manual dispatch is
   optional; it only validates, and additionally compares its rebuilt payloads
   with a prior successful push preflight at the same commit.
3. The `publisher` job uses the repository-scoped ephemeral `github.token`,
   with `contents: write` and `actions: read`, and no environment or external
   secret. It creates, updates, and deletes a disposable private draft (no
   assets or tag), proving that the actual job token can manage releases. Ref
   lookups assert no probe tag was created. Organizations must permit Actions,
   these runners/actions, and the job's write token. A denied token or approval
   is a blocking error; local gh credentials are not proof.
4. Merge the PR normally, respecting approvals and checks. The `CI` and
   `release` push runs at the merged commit are the release preflight; wait
   for both to succeed. No manual workflow dispatch is required.

   Optionally, verify the same evidence locally from a checkout detached at
   the merged commit (these commands never publish):

   ```sh
   export RELEASE_COMMIT=$(git rev-parse HEAD)
   export RELEASE_TAG=vX.Y.Z
   python3 scripts/release.py build
   python3 scripts/release.py version
   ```

   Replace `vX.Y.Z` with the proposed version's tag. They require successful
   exact-SHA `CI` and `release` push runs, inspect the publisher steps and
   unexpired Actions artifacts, validate all packaged metadata, and check the
   tag/release namespace and any existing assets. Authentication, network
   errors, missing/expired evidence, skipped jobs, and conflicting versions
   fail closed. Artifact retention is 30 days; rerun the unchanged commit's
   non-publishing workflow if evidence expires.

## Publication (separate authorization/phase)

Pushing the matching annotated tag at the merged commit is the only release
action; everything else runs in the tag workflow:

```sh
git tag -a vX.Y.Z -m "dsh-acp vX.Y.Z"
git push origin vX.Y.Z
```

Push it from an authorized CLI identity; do not assume tags pushed with
GITHUB_TOKEN will trigger Actions. Never create/push tags during preflight.
The publication workflow builds all platforms before its publisher starts.
Release builds pin Rust 1.99.1. Both Windows targets use the MSVC `/Brepro`
linker option to avoid changing PE timestamps and identifiers (see [LLVM's
reproducible-linking notes](https://blog.llvm.org/2019/11/deterministic-builds-with-clang-and-lld.html)).
The Windows x64 runner has no NASM, so release builds set
`AWS_LC_SYS_PREBUILT_NASM=1` and `aws-lc-sys` uses its checked-in prebuilt
NASM objects. Before uploading, the tag run must compare every unpacked payload
with the successful branch preflight artifacts at the same commit. A
runner/toolchain change producing different binaries is a blocking error,
never an upload followed by a failed verification. The publisher's read-only
Actions permission is used to retrieve that exact-commit evidence. Before the
first release asset upload, it validates all artifacts, exact
version/tag/commit, existing release contents, and its own fresh publishing
credential. It creates missing draft state with generated release notes,
uploads only missing assets, reads each new upload back and compares it to the
exact staged bytes, verifies completeness, then makes the release public. No
clobber or deletion of conflicting assets is permitted.

Recovery accepts existing immutable artifacts only after validating their own
checksums and comparing unpacked bytes, executable permissions and commit/
version/platform metadata to the staged build. Compression differences alone
are acceptable; binary differences are not. For a partial draft archive missing
its checksum, validate the archive contents against the staged build, retain
its exact uploaded bytes, and add a checksum for those bytes; never replace the
archive just because compression differs. Already-public releases are verified
read-only by the publication command. Never move tags or replace assets of a
completed release.

After publishing, the tag run's **Verify the published release** step runs
`python3 scripts/release.py published`. It requires a public release, the exact
tag commit and all twelve assets, validates every checksum and archive member,
and compares payloads with the preflight build. The same command can be rerun
locally with `RELEASE_COMMIT` and `RELEASE_TAG` set. Both `ci.yml` and
`release.yml` must also succeed in the tag push context; branch evidence cannot
replace tag workflow verification. Edit the GitHub release notes to match the
changelog section if the generated notes are not enough.

The tag run must also complete **Publish npm package with trusted publishing**.
That step uses `id-token: write` for npm OIDC, publishes stable versions to
`latest` and prereleases to `next`, and verifies registry integrity. If npm
publication fails after GitHub succeeds, follow the npm guide and rerun the
same tag workflow; do not replace the completed GitHub assets or reuse the
version for different package contents. The first npm publication is manual;
see [npm publishing](docs/npm-publishing.md).

## Release draft lookups

Draft discovery uses the authenticated paginated release list and GraphQL when
REST tag lookup returns 404; upload readback and final completeness checks use
the known release ID. Release PATCH requests explicitly preserve `tag_name`;
omitting it can rename a draft to an untagged placeholder. The publisher probe
validates discovery after its update, as well as reading the updated body back
through the release ID.
