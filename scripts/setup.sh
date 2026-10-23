#!/usr/bin/env bash
# One-time setup: install dependencies and create .env with a random DB password.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v uv >/dev/null || { echo "uv not found: https://docs.astral.sh/uv/"; exit 1; }
command -v docker >/dev/null || { echo "docker not found"; exit 1; }

uv sync

if [[ ! -f .env ]]; then
    password="$(uv run python -c 'import secrets; print(secrets.token_hex(16))')"
    sed "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${password}/" .env.example > .env
    echo "created .env with a generated POSTGRES_PASSWORD"
else
    echo ".env already exists; leaving it alone"
fi
