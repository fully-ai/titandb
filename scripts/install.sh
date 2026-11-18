#!/usr/bin/env bash
set -euo pipefail
command -v cargo >/dev/null 2>&1 || { echo "Error: Rust/Cargo required." >&2; exit 1; }
cd "$(dirname "$0")/.."
cargo build --release --bins
