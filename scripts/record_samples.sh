#!/usr/bin/env bash
# Capture real exchange messages as test fixtures (tests/fixtures/<exchange>.tsv.gz).
# Fixtures are real recordings, never hand-written; re-run to refresh them.
set -euo pipefail
cd "$(dirname "$0")/.."

seconds="${1:-45}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

uv run python -m ticksentinel.ingest --no-db --duration "$seconds" --record-dir "$tmp"

for exchange in binance coinbase kraken; do
    src="$(ls "$tmp"/"$exchange"-*.tsv.gz)"
    mv "$src" "tests/fixtures/${exchange}.tsv.gz"
    echo "tests/fixtures/${exchange}.tsv.gz: $(gzip -dc "tests/fixtures/${exchange}.tsv.gz" | wc -l) messages"
done
