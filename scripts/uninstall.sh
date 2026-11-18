#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target/
echo "Removed ./target build artifacts (user data preserved)."
