# Releasing dsh-acp

Releases use annotated `v<version>` Git tags and GitHub Releases. The package is
a native Rust binary; source installation remains available through
`cargo install --git`.

## Preparation

1. Start from an up-to-date `master` with a clean worktree.
2. Update the version in `Cargo.toml` and `Cargo.lock`.
3. Move the `Unreleased` entries in `CHANGELOG.md` under the new version.
4. Run the complete contribution checks.
5. Run `cargo build --release --locked` and verify the binary.
6. Commit the release preparation and wait for CI to pass at that exact commit.

## Publication

Create and push an annotated tag from the verified commit:

```bash
git tag -a vX.Y.Z -m "dsh-acp vX.Y.Z"
git push origin vX.Y.Z
```

Create the GitHub Release from the tag and use the matching changelog section
as release notes. Do not reuse a version for different contents.

After publication, verify that the README release badge points to the new
release and that a clean checkout can run:

```bash
cargo test --locked
cargo install --git https://github.com/BrokkAi/dsh-acp --version X.Y.Z
```
