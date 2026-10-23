# Production image for Render. Built and tested locally with:
#   docker compose --profile app up --build
FROM python:3.12-slim

# Pinned uv, same version as development, so the lockfile resolves identically.
COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Dependencies first, in their own layer: code edits don't reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

# Don't run as root inside the container.
RUN useradd --create-home app && mkdir -p /app/logs && chown app /app/logs
USER app

# No raw recordings in production: Render's free disk is wiped on every restart,
# and recordings are a local development tool. PORT is injected by Render.
CMD ["python", "-m", "ticksentinel.ingest", "--no-record"]
