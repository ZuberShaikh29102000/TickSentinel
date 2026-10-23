"""Recovery against the REAL exchanges, with faults injected on a live connection.

Exchanges can't be made to lose packets on demand, so we do it ourselves on a
real session: either silently drop one message (as if lost in transit) or abort
the TCP connection (as if the network failed). Recovery then goes through the
real code paths: Binance refetches its REST snapshot, Coinbase and Kraken
reconnect and get a fresh book from the exchange.

Run with:  uv run pytest -m live -v
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection

from tests.conftest import Collector
from ticksentinel.common.models import FeedEventType, Tick, TickKind
from ticksentinel.ingest.backoff import Backoff
from ticksentinel.ingest.base import FeedClient
from ticksentinel.ingest.binance import BinanceClient
from ticksentinel.ingest.coinbase import CoinbaseClient
from ticksentinel.ingest.kraken import KrakenClient
from ticksentinel.ingest.pipeline import Pipeline

pytestmark = pytest.mark.live

UPDATES_AFTER_RECOVERY = 30  # recovered = a fresh snapshot plus this many good updates after it


class FaultInjector:
    """Wraps a client: on the Nth message matching `match`, apply `fault` once.

    With `rearm_unless`, a drop that the client hasn't noticed after
    `rearm_after` further matching messages is counted as self-healed and another
    drop is injected. Needed for Kraken: a lost update to a level that is later
    overwritten (absolute quantities) leaves the book correct, so no checksum
    mismatch can, or should, fire.
    """

    def __init__(
        self,
        client: FeedClient,
        *,
        fault: str,
        after: int,
        match: Callable[[str], bool],
        rearm_unless: Callable[[], bool] | None = None,
        rearm_after: int = 20,
    ) -> None:
        self.client, self.fault, self.after, self.match = client, fault, after, match
        self.rearm_unless, self.rearm_after = rearm_unless, rearm_after
        self.seen = 0
        self.fired = False
        self.drops = 0
        self.self_healed = 0
        self._since_fire = 0
        self.ws: ClientConnection | None = None
        self._on_message = client.on_message
        self._subscribe = client.subscribe
        client.on_message = self.on_message  # type: ignore[method-assign]
        client.subscribe = self.subscribe  # type: ignore[method-assign]

    async def subscribe(self, ws: ClientConnection) -> None:
        self.ws = ws
        await self._subscribe(ws)

    def on_message(self, raw: str, recv_ns: int) -> None:
        if self.fired and self.rearm_unless is not None and self.match(raw):
            self._since_fire += 1
            if self._since_fire > self.rearm_after and not self.rearm_unless():
                # Every checksum since the drop matched Kraken: the book healed itself.
                self.self_healed += 1
                self.fired, self.seen, self.after, self._since_fire = False, 0, 1, 0
        if not self.fired and self.match(raw):
            self.seen += 1
            if self.seen == self.after:
                self.fired = True
                if self.fault == "drop":
                    self.drops += 1
                    return  # message "lost in transit"
                if self.fault == "abort":
                    assert self.ws is not None
                    # Hard network failure, no close handshake. This message arrived
                    # intact, so still process it: an abort must be the ONLY fault.
                    self.ws.transport.abort()
        self._on_message(raw, recv_ns)


class Harness:
    def __init__(self, cls: type[FeedClient]) -> None:
        self.collector = Collector()
        self.pipeline = Pipeline(depth=10)
        self.tops: list[Any] = []
        self.client = cls(
            self.on_tick, self.collector.on_event, backoff=Backoff(base_s=0.2, cap_s=1.0)
        )

    def on_tick(self, tick: Tick) -> None:
        self.collector.on_tick(tick)
        self.pipeline.on_tick(tick)
        if tick.kind is not TickKind.TRADE:
            self.tops.append(self.pipeline.last_top[(tick.exchange, tick.symbol)])

    def book_ticks(self) -> list[Tick]:
        return [t for t in self.collector.ticks if t.kind is not TickKind.TRADE]

    def recovered(self) -> bool:
        """A second snapshot arrived and the book kept updating normally after it."""
        book = self.book_ticks()
        snaps = [i for i, t in enumerate(book) if t.kind is TickKind.BOOK_SNAPSHOT]
        return len(snaps) >= 2 and len(book) - snaps[1] - 1 >= UPDATES_AFTER_RECOVERY

    async def run_until_recovered(self, timeout_s: float = 90) -> None:
        task = asyncio.create_task(self.client.run())
        try:
            async with asyncio.timeout(timeout_s):
                while not self.recovered():
                    await asyncio.sleep(0.05)
        except TimeoutError:
            book = self.book_ticks()
            snaps = [i for i, t in enumerate(book) if t.kind is TickKind.BOOK_SNAPSHOT]
            events = [(e.event_type.value, dict(e.detail)) for e in self.collector.events]
            raise AssertionError(
                f"{self.client.exchange} not recovered in {timeout_s}s: "
                f"msgs={self.client.messages_received} book_ticks={len(book)} "
                f"snapshot_idx={snaps} events={events}"
            ) from None
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    def assert_books_sane(self) -> None:
        assert self.tops
        for top in self.tops:
            assert top.bids and top.asks
            assert top.bids[0][0] < top.asks[0][0], f"crossed book at seq {top.seq}"

    def events(self, event_type: FeedEventType) -> list[Any]:
        return [e for e in self.collector.events if e.event_type is event_type]


# ---- message gaps ---------------------------------------------------------------


async def test_binance_live_gap_rebuilds_from_rest_without_reconnecting() -> None:
    h = Harness(BinanceClient)
    client = h.client
    assert isinstance(client, BinanceClient)
    FaultInjector(
        client,
        fault="drop",
        after=20,
        match=lambda raw: '"depthUpdate"' in raw and client.sequencer.synced,
    )
    await h.run_until_recovered()

    (gap,) = h.events(FeedEventType.SEQUENCE_GAP)
    assert gap.detail["missed"] > 0
    assert len(h.events(FeedEventType.RESYNC)) == 1
    assert h.events(FeedEventType.DISCONNECTED) == []  # recovered in place, per Binance's docs
    assert client.sequencer.synced
    h.assert_books_sane()


async def test_coinbase_live_gap_reconnects_and_resubscribes() -> None:
    h = Harness(CoinbaseClient)
    FaultInjector(h.client, fault="drop", after=50, match=lambda raw: True)
    await h.run_until_recovered()

    (gap,) = h.events(FeedEventType.SEQUENCE_GAP)
    assert gap.detail["missed"] == 1
    (disc,) = h.events(FeedEventType.DISCONNECTED)
    assert disc.detail["reason"].startswith("resync required: sequence gap")
    assert len(h.events(FeedEventType.CONNECTED)) == 2
    h.assert_books_sane()


async def test_kraken_live_lost_update_caught_by_checksum() -> None:
    h = Harness(KrakenClient)
    client = h.client
    assert isinstance(client, KrakenClient)
    injector = FaultInjector(
        client,
        fault="drop",
        after=20,
        match=lambda raw: raw.startswith('{"channel":"book","type":"update"'),
        rearm_unless=lambda: bool(h.events(FeedEventType.CHECKSUM_MISMATCH)),
    )
    await h.run_until_recovered()

    # Every drop was either caught by the checksum (exactly one; then we stop
    # dropping) or provably self-healed (20+ later checksums all matched Kraken).
    assert injector.drops == injector.self_healed + 1
    (mismatch,) = h.events(FeedEventType.CHECKSUM_MISMATCH)
    assert mismatch.detail["expected"] != mismatch.detail["computed"]
    (disc,) = h.events(FeedEventType.DISCONNECTED)
    assert disc.detail["reason"].startswith("resync required: checksum mismatch")
    assert client.checksums_verified >= UPDATES_AFTER_RECOVERY  # post-reconnect book matches Kraken
    h.assert_books_sane()


# ---- network failure (also the path Binance's 24 h disconnect takes) -----------


@pytest.mark.parametrize("cls", [BinanceClient, CoinbaseClient, KrakenClient])
async def test_live_connection_abort_reconnects_with_fresh_book(cls: type[FeedClient]) -> None:
    h = Harness(cls)
    FaultInjector(h.client, fault="abort", after=40, match=lambda raw: True)
    await h.run_until_recovered()

    (disc,) = h.events(FeedEventType.DISCONNECTED)
    assert disc.detail["reason"].startswith("network"), disc.detail["reason"]
    assert len(h.events(FeedEventType.CONNECTED)) == 2
    assert h.client.sessions_started == 2
    # Only the network fault: frames already buffered before the abort must not
    # register as data loss.
    assert not h.events(FeedEventType.SEQUENCE_GAP) and not h.events(
        FeedEventType.CHECKSUM_MISMATCH
    )
    h.assert_books_sane()
