"""Reconnect loop against a real local websocket server that misbehaves on purpose."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator

import pytest
from websockets.asyncio.server import ServerConnection, serve

from tests.conftest import Collector
from ticksentinel.common.models import Exchange, FeedEventType
from ticksentinel.ingest.backoff import Backoff
from ticksentinel.ingest.base import FeedClient, ResyncRequired


class FakeClient(FeedClient):
    exchange = Exchange.KRAKEN

    def __init__(
        self,
        url: str,
        collector: Collector,
        failure: str | None = None,
        idle_timeout_s: float = 15.0,
    ) -> None:
        super().__init__(
            collector.on_tick,
            collector.on_event,
            backoff=Backoff(base_s=0.01, cap_s=0.02, rng=random.Random(0)),
            idle_timeout_s=idle_timeout_s,
        )
        self._url = url
        self.failure = failure  # None | "resync" | "bug": what happens on message "a"
        self.resets = 0
        self.payloads: list[str] = []

    @property
    def url(self) -> str:
        return self._url

    def reset_state(self) -> None:
        self.resets += 1

    def on_message(self, raw: str, recv_ns: int) -> None:
        self.payloads.append(raw)
        if raw == "a" and self.failure == "resync":
            raise ResyncRequired("book untrusted")
        if raw == "a" and self.failure == "bug":
            raise ValueError("simulated parsing bug")


async def send_two_then_close(ws: ServerConnection) -> None:
    await ws.send("a")
    await ws.send("b")
    await ws.close()


@pytest.fixture
async def server_url() -> AsyncIterator[str]:
    async with serve(send_two_then_close, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}"


async def run_until_disconnects(client: FakeClient, collector: Collector, n: int) -> None:
    task = asyncio.create_task(client.run())
    try:
        async with asyncio.timeout(10):
            while collector.event_types().count(FeedEventType.DISCONNECTED) < n:
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def disconnect_reasons(collector: Collector) -> list[str]:
    return [
        e.detail["reason"] for e in collector.events if e.event_type is FeedEventType.DISCONNECTED
    ]


async def test_reconnects_after_server_closes(server_url: str, collector: Collector) -> None:
    client = FakeClient(server_url, collector)
    await run_until_disconnects(client, collector, 2)

    assert client.payloads[:4] == ["a", "b", "a", "b"]
    assert client.resets >= 2  # state wiped before every session
    assert collector.event_types()[:4] == [
        FeedEventType.CONNECTED,
        FeedEventType.DISCONNECTED,
        FeedEventType.CONNECTED,
        FeedEventType.DISCONNECTED,
    ]
    first = next(e for e in collector.events if e.event_type is FeedEventType.DISCONNECTED)
    assert first.detail["reason"] == "closed by server"
    assert 0 <= first.detail["reconnect_in_s"] <= 0.02


async def test_resync_request_reconnects(server_url: str, collector: Collector) -> None:
    client = FakeClient(server_url, collector, failure="resync")
    await run_until_disconnects(client, collector, 2)
    assert all(
        r.startswith("resync required: book untrusted") for r in disconnect_reasons(collector)
    )
    assert "b" not in client.payloads  # session abandoned at the bad message


async def test_handler_bug_is_logged_and_survived(server_url: str, collector: Collector) -> None:
    client = FakeClient(server_url, collector, failure="bug")
    await run_until_disconnects(client, collector, 2)
    assert all(r.startswith("handler error: ValueError") for r in disconnect_reasons(collector))
    errors = [e for e in collector.events if e.event_type is FeedEventType.ERROR]
    assert (
        errors and errors[0].detail["exception"] == "ValueError"
    )  # possible INTERNAL_BUG evidence
    assert client.sessions_started >= 2  # the handler kept going


async def test_connection_refused_keeps_retrying(collector: Collector) -> None:
    client = FakeClient("ws://127.0.0.1:1", collector)  # nothing listens on port 1
    await run_until_disconnects(client, collector, 3)
    assert all(r.startswith("network") for r in disconnect_reasons(collector))
    assert FeedEventType.CONNECTED not in collector.event_types()


async def send_one_then_go_silent(ws: ServerConnection) -> None:
    """Connection stays open (pings keep working) but data stops: the failure
    mode that no exception ever reveals."""
    await ws.send("a")
    await ws.wait_closed()  # silent until the client gives up and disconnects


async def test_watchdog_reconnects_a_silent_feed(collector: Collector) -> None:
    async with serve(send_one_then_go_silent, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = FakeClient(f"ws://127.0.0.1:{port}", collector, idle_timeout_s=0.3)
        await run_until_disconnects(client, collector, 2)

    assert all(r.startswith("watchdog: no data") for r in disconnect_reasons(collector))
    assert collector.event_types().count(FeedEventType.WATCHDOG_TIMEOUT) >= 2
    assert client.payloads[:2] == ["a", "a"]  # it got data again after each forced reconnect


async def test_watchdog_does_not_fire_while_data_flows(collector: Collector) -> None:
    async def steady(ws: ServerConnection) -> None:
        for _ in range(10):
            await ws.send("x")
            await asyncio.sleep(0.1)  # gaps shorter than the idle timeout
        await ws.close()

    async with serve(steady, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = FakeClient(f"ws://127.0.0.1:{port}", collector, idle_timeout_s=0.3)
        await run_until_disconnects(client, collector, 1)

    assert disconnect_reasons(collector)[0] == "closed by server"  # ran a full second, no watchdog
    assert FeedEventType.WATCHDOG_TIMEOUT not in collector.event_types()
