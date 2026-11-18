#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo run --release --bin titandb -- --data-dir "${TITANDB_DATA:-data/db}" --wire-addr "${TITANDB_WIRE:-127.0.0.1:7440}" --http-addr "${TITANDB_HTTP:-127.0.0.1:7441}"
