$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
