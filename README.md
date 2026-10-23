# TickSentinel

Live monitor for crypto market data feeds (Binance, Coinbase, Kraken) that detects
broken data within microseconds and uses AI agents to diagnose the root cause:
exchange-side fault, network loss, internal bug, or a real market event.

> Work in progress. Ingestion, storage and live deployment are in place; the full
> README with architecture, design decisions and results arrives in Phase 7.

## Run locally

```bash
scripts/setup.sh                 # uv sync + .env with a generated DB password
scripts/start.sh                 # local Postgres + live ingestion from all 3 exchanges
uv run python -m ticksentinel.storage.check   # rows per feed + storage used
uv run pytest                    # unit + integration tests (DB tests skip if Postgres is down)
uv run pytest -m live            # recovery tests against the real exchanges (~1-2 min)
```

Health endpoint while running: http://localhost:8080/health

## Run the production container locally

```bash
docker compose --profile app up --build   # the exact image Render runs, on :10000
```

## Deploy

See [docs/DEPLOY.md](docs/DEPLOY.md): Render free + Supabase free, $0.
