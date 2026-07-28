# v0.30.0 Release Checklist

## Required quality gates

```text
[ ] cargo fmt --all -- --check
[ ] cargo check --workspace --all-features
[ ] cargo clippy --workspace --all-targets --all-features -- -D warnings
[ ] cargo test --workspace --all-features
[ ] cargo doc --workspace --all-features --no-deps
[ ] cargo run --release -- --validate-v30
[ ] cargo publish --dry-run -p delta-stream-derive
[ ] cargo publish --dry-run -p delta-stream
```

## Release order

`delta-stream` has an optional versioned dependency on `delta-stream-derive`. For a crates.io release, publish `delta-stream-derive` first, wait until the registry can resolve it, then publish `delta-stream`.

## Before first crates.io publication

- verify the crate names are available/owned by the maintainer;
- add the final public repository URL to both manifests;
- add a real security-reporting contact/process;
- inspect `cargo package --list`;
- inspect generated `.crate` size;
- run `cargo publish --dry-run` for both packages;
- tag the exact commit as `v0.30.0` after all gates pass.

Cargo's publish operation is permanent for a particular version, so do not publish until metadata and package contents are final.
