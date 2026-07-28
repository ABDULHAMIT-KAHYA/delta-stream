$ErrorActionPreference = "Stop"

Write-Host "== DeltaStream v0.30.0 release checks ==" -ForegroundColor Cyan

cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo run --release -- --validate-v30

Write-Host "== Package dry-run ==" -ForegroundColor Cyan
Write-Host "Run these after the public repository metadata and crates.io names are final:"
Write-Host "  cargo publish --dry-run -p delta-stream-derive"
Write-Host "  cargo publish --dry-run -p delta-stream"

Write-Host "All local release checks passed." -ForegroundColor Green
