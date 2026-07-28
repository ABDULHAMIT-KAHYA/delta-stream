# Contributing

Thanks for helping improve DeltaStream.

Before opening a pull request, run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

Protocol changes should include tests for malformed input, duplicate/stale behavior, recovery behavior, and backwards-compatibility implications.

Benchmark changes should document the workload and avoid presenting synthetic results as universal performance claims.
