#!/usr/bin/env bash
# Start local Postgres and stream all three exchanges into it.
# Extra args go to the ingester, e.g.:  scripts/start.sh --duration 120
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo ".env missing: run scripts/setup.sh first"; exit 1; }

docker compose up -d --wait postgres
exec uv run python -m ticksentinel.ingest "$@"
