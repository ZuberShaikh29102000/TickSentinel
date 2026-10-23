"""/health: is the live system working, and if not, which part?

Three consumers: Render (requires every web service to bind an HTTP port), the
external keep-alive pinger (cron-job.org hits it every 10 minutes so the free
instance never sleeps), and humans checking on the deployment.

It always answers HTTP 200 and reports problems in the body. A 5xx would make
Render treat the instance as unhealthy and restart it, but most problems here
(an exchange outage, the database down) are outside the process and are already
handled by reconnect logic; a restart would only add a gap.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import uvicorn
from fastapi import FastAPI

from ticksentinel.ingest.base import DEFAULT_IDLE_TIMEOUT_S, FeedClient
from ticksentinel.storage.maintenance import Maintainer
from ticksentinel.storage.writer import DbWriter

StatusFn = Callable[[], dict[str, Any]]


def feed_status(client: FeedClient, now_monotonic: float) -> dict[str, Any]:
    last = client.last_message_monotonic
    age = None if last is None else round(now_monotonic - last, 3)
    healthy = client.connected and age is not None and age < DEFAULT_IDLE_TIMEOUT_S
    return {
        "ok": healthy,
        "connected": client.connected,
        "last_message_age_s": age,
        "messages": client.messages_received,
        "sessions": client.sessions_started,
    }


def build_health(
    clients: Sequence[FeedClient],
    writer: DbWriter | None,
    maintainer: Maintainer | None,
    started_monotonic: float,
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    now = time.monotonic() if now_monotonic is None else now_monotonic
    feeds = {c.exchange.value: feed_status(c, now) for c in clients}
    db: dict[str, Any] = {"enabled": writer is not None}
    if writer is not None and writer.db is not None:
        db |= {
            "connected": writer.db.pool is not None,
            "last_error": writer.db.last_error,
            "pending_rows": writer.pending(),
            "written": dict(writer.written),
            "dropped": dict(writer.dropped),
        }
    if maintainer is not None:
        db["maintenance"] = {
            "runs": maintainer.state.runs,
            "last_result": maintainer.state.last_result,
            "last_error": maintainer.last_error,
        }
    all_ok = all(f["ok"] for f in feeds.values()) and db.get("connected", True)
    return {
        "status": "ok" if all_ok else "degraded",
        "uptime_s": round(now - started_monotonic, 1),
        "feeds": feeds,
        "db": db,
    }


def create_app(status: StatusFn) -> FastAPI:
    app = FastAPI(title="TickSentinel", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return status()

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "TickSentinel", "health": "/health"}

    return app


class _EmbeddedServer(uvicorn.Server):
    """uvicorn normally takes over SIGINT/SIGTERM; inside our own asyncio app the
    main loop owns shutdown, so signal capture is disabled."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


async def serve(status: StatusFn, port: int) -> None:
    config = uvicorn.Config(
        create_app(status), host="0.0.0.0", port=port, log_level="warning", lifespan="off"
    )
    await _EmbeddedServer(config).serve()
