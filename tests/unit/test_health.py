"""/health payload and HTTP endpoint."""

from __future__ import annotations

import httpx

from tests.conftest import Collector
from ticksentinel.ingest.kraken import KrakenClient
from ticksentinel.ops.health import build_health, create_app
from ticksentinel.storage.writer import DbWriter


def client_with(connected: bool, last_msg: float | None) -> KrakenClient:
    c = Collector()
    client = KrakenClient(c.on_tick, c.on_event)
    client.connected = connected
    client.last_message_monotonic = last_msg
    client.messages_received = 42
    return client


def test_all_feeds_fresh_is_ok() -> None:
    health = build_health(
        [client_with(True, 99.0)], None, None, started_monotonic=0.0, now_monotonic=100.0
    )
    assert health["status"] == "ok"
    assert health["uptime_s"] == 100.0
    feed = health["feeds"]["kraken"]
    assert feed == {
        "ok": True,
        "connected": True,
        "last_message_age_s": 1.0,
        "messages": 42,
        "sessions": 0,
    }


def test_stale_or_disconnected_feed_is_degraded() -> None:
    stale = build_health([client_with(True, 50.0)], None, None, 0.0, now_monotonic=100.0)
    assert stale["status"] == "degraded" and not stale["feeds"]["kraken"]["ok"]
    down = build_health([client_with(False, 99.0)], None, None, 0.0, now_monotonic=100.0)
    assert down["status"] == "degraded"
    never = build_health([client_with(True, None)], None, None, 0.0, now_monotonic=100.0)
    assert never["feeds"]["kraken"]["last_message_age_s"] is None


def test_database_down_is_degraded_and_reported() -> None:
    class DownDb:
        pool = None
        last_error = "OSError: connection refused"

    writer = DbWriter(DownDb())  # type: ignore[arg-type]  # duck-typed fake
    health = build_health([client_with(True, 99.5)], writer, None, 0.0, now_monotonic=100.0)
    assert health["status"] == "degraded"
    assert health["db"]["connected"] is False
    assert health["db"]["last_error"] == "OSError: connection refused"


async def test_endpoint_always_answers_200_with_status_in_body() -> None:
    """Even when degraded: a 5xx would make Render restart a process whose
    problem is outside it (e.g. an exchange outage)."""
    app = create_app(lambda: {"status": "degraded", "feeds": {}})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        resp = await http.get("/health")
        root = await http.get("/")
    assert resp.status_code == 200 and resp.json()["status"] == "degraded"
    assert root.json()["health"] == "/health"
