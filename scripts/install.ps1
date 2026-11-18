$ErrorActionPreference = "Stop"
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { Write-Error "Rust/Cargo required."; exit 1 }
Set-Location (Join-Path $PSScriptRoot "..")
cargo build --release --bins
