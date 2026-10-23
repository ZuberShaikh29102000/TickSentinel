"""Binance spot feed handler (diff depth + trades).

Binance's diff-depth stream only sends *changes*, so a correct book needs a
REST snapshot stitched onto the stream using update IDs. This follows the
official "How to manage a local order book correctly" procedure:
https://developers.binance.com/en/docs/products/spot/web-socket-streams

  1. Open the stream and buffer depth events; note the first event's U.
  2. GET /api/v3/depth for a snapshot.
  3. If snapshot.lastUpdateId < first buffered U, the snapshot is too old: refetch.
  4. Drop buffered events with u <= lastUpdateId.
  5. Apply the rest. For every event: u < book_id -> ignore;
     U > book_id + 1 -> we MISSED events: discard the book and restart.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import Any

import httpx
import orjson

from ticksentinel.common.models import (
    Exchange,
    FeedEventType,
    Level,
    Side,
    Tick,
    TickKind,
    Trade,
)
from ticksentinel.ingest.base import FeedClient, ResyncRequired

log = logging.getLogger(__name__)

WS_BASE = "wss://stream.binance.com:9443"
REST_DEPTH_URL = "https://api.binance.com/api/v3/depth"
MAX_SNAPSHOT_ATTEMPTS = 5
SNAPSHOT_RETRY_DELAY_S = 0.5


class Verdict(Enum):
    APPLY = "apply"
    STALE = "stale"  # already covered by the snapshot / earlier events
    GAP = "gap"  # events were missed; the book is wrong


class DepthSequencer:
    """Pure (IO-free) implementation of Binance's snapshot/diff stitching rules."""

    def __init__(self) -> None:
        self.book_update_id: int | None = None
        self.buffer: list[tuple[dict[str, Any], int]] = []  # (event, recv_ns) while syncing

    @property
    def synced(self) -> bool:
        return self.book_update_id is not None

    def restart(self) -> None:
        self.book_update_id = None
        self.buffer = []

    def on_snapshot(self, last_update_id: int) -> list[tuple[dict[str, Any], int]] | None:
        """Adopt a snapshot. Returns buffered events to run through check(), or None
        if the snapshot predates the buffered stream and must be refetched (step 3)."""
        if not self.buffer or last_update_id < self.buffer[0][0]["U"]:
            return None
        self.book_update_id = last_update_id
        pending, self.buffer = self.buffer, []
        return pending

    def check(self, event: dict[str, Any]) -> Verdict:
        assert self.book_update_id is not None, "check() before a snapshot was adopted"
        if event["u"] <= self.book_update_id:
            return Verdict.STALE
        if event["U"] > self.book_update_id + 1:
            return Verdict.GAP
        self.book_update_id = event["u"]
        return Verdict.APPLY


def _levels(pairs: list[list[str]]) -> tuple[Level, ...]:
    return tuple(Level(float(p), float(q)) for p, q in pairs)


class BinanceClient(FeedClient):
    exchange = Exchange.BINANCE

    def __init__(
        self,
        *args: Any,
        symbol: str = "BTCUSDT",
        canonical_symbol: str = "BTC-USDT",
        snapshot_limit: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.symbol = symbol
        self.canonical_symbol = canonical_symbol
        # 1000 levels per side is plenty for top-of-book work and costs 50 request
        # weight vs 250 for the 5000-level snapshot.
        self.snapshot_limit = snapshot_limit
        self.sequencer = DepthSequencer()
        self._snapshot_task: asyncio.Task[None] | None = None
        self._snapshot_error: BaseException | None = None

    @property
    def url(self) -> str:
        s = self.symbol.lower()
        # Combined stream: one connection for depth + trades. MICROSECOND gives us
        # 1000x finer exchange timestamps than the millisecond default.
        return f"{WS_BASE}/stream?streams={s}@depth@100ms/{s}@trade&timeUnit=MICROSECOND"

    def reset_state(self) -> None:
        self.sequencer.restart()
        self._snapshot_task = None
        self._snapshot_error = None

    async def on_session_end(self) -> None:
        if self._snapshot_task is not None and not self._snapshot_task.done():
            self._snapshot_task.cancel()

    # ---- message handling (sync, replayable) --------------------------------

    def on_message(self, raw: str, recv_ns: int) -> None:
        if self._snapshot_error is not None:
            raise ResyncRequired(f"snapshot fetch failed: {self._snapshot_error!r}")

        data = orjson.loads(raw)["data"]
        event = data.get("e")
        if event == "depthUpdate":
            self._on_depth(data, recv_ns)
        elif event == "trade":
            self._on_trade(data, recv_ns)
        elif event == "serverShutdown":
            # Binance is about to drop us; reconnect now rather than wait for the error.
            self.emit_event(FeedEventType.SERVER_SHUTDOWN, {"E": data.get("E")})
            raise ResyncRequired("serverShutdown event")

    def on_rest_snapshot(self, raw: str, recv_ns: int) -> bool:
        """Adopt a REST depth snapshot. Returns False if it was too old to use."""
        snap = orjson.loads(raw)
        pending = self.sequencer.on_snapshot(snap["lastUpdateId"])
        if pending is None:
            return False
        self.emit_tick(
            Tick(
                exchange=self.exchange,
                symbol=self.canonical_symbol,
                kind=TickKind.BOOK_SNAPSHOT,
                seq=snap["lastUpdateId"],
                # The REST snapshot carries no exchange timestamp; receive time is the
                # best available bound on when it was valid.
                exch_ts_ns=recv_ns,
                recv_ts_ns=recv_ns,
                bids=_levels(snap["bids"]),
                asks=_levels(snap["asks"]),
            )
        )
        for event, event_recv_ns in pending:
            # Via _on_depth: if a gap appears mid-way, later events get re-buffered.
            self._on_depth(event, event_recv_ns)
        return True

    def _on_depth(self, event: dict[str, Any], recv_ns: int) -> None:
        if not self.sequencer.synced:
            self.sequencer.buffer.append((event, recv_ns))
            self._request_snapshot()
            return
        self._apply_or_resync(event, recv_ns)

    def _apply_or_resync(self, event: dict[str, Any], recv_ns: int) -> None:
        expected = self.sequencer.book_update_id
        verdict = self.sequencer.check(event)
        if verdict is Verdict.APPLY:
            self.emit_tick(
                Tick(
                    exchange=self.exchange,
                    symbol=self.canonical_symbol,
                    kind=TickKind.BOOK_UPDATE,
                    seq=event["u"],
                    exch_ts_ns=event["E"] * 1_000,  # µs -> ns
                    recv_ts_ns=recv_ns,
                    bids=_levels(event["b"]),
                    asks=_levels(event["a"]),
                )
            )
        elif verdict is Verdict.GAP:
            # No reconnect needed: Binance's documented recovery is to rebuild the
            # book from a new snapshot while continuing to buffer the live stream.
            assert expected is not None
            self.emit_event(
                FeedEventType.SEQUENCE_GAP,
                {
                    "expected_U": expected + 1,
                    "got_U": event["U"],
                    "missed": event["U"] - expected - 1,
                },
            )
            self.emit_event(FeedEventType.RESYNC, {"reason": "sequence_gap"})
            self.sequencer.restart()
            self._snapshot_task = None
            self.sequencer.buffer.append((event, recv_ns))
            self._request_snapshot()

    def _on_trade(self, data: dict[str, Any], recv_ns: int) -> None:
        trade = Trade(
            trade_id=str(data["t"]),
            price=float(data["p"]),
            qty=float(data["q"]),
            # m = "is the buyer the market maker?" If the buyer was the maker,
            # the seller was the aggressor.
            side=Side.SELL if data["m"] else Side.BUY,
            exch_ts_ns=data["T"] * 1_000,
        )
        self.emit_tick(
            Tick(
                exchange=self.exchange,
                symbol=self.canonical_symbol,
                kind=TickKind.TRADE,
                seq=data["t"],
                exch_ts_ns=data["E"] * 1_000,
                recv_ts_ns=recv_ns,
                trades=(trade,),
            )
        )

    # ---- snapshot fetching (async IO) ---------------------------------------

    def _request_snapshot(self) -> None:
        if self._snapshot_task is None:
            self._snapshot_task = asyncio.get_running_loop().create_task(self._fetch_snapshot())

    async def _fetch_snapshot(self) -> None:
        params: dict[str, str | int] = {"symbol": self.symbol, "limit": self.snapshot_limit}
        try:
            async with httpx.AsyncClient(timeout=10) as http:
                for attempt in range(1, MAX_SNAPSHOT_ATTEMPTS + 1):
                    resp = await http.get(REST_DEPTH_URL, params=params)
                    recv_ns = self._clock()
                    resp.raise_for_status()
                    self.record_raw("rest", resp.text, recv_ns)
                    if self.on_rest_snapshot(resp.text, recv_ns):
                        log.info("binance book synced (attempt %d)", attempt)
                        return
                    await asyncio.sleep(SNAPSHOT_RETRY_DELAY_S)
            raise RuntimeError(f"snapshot still stale after {MAX_SNAPSHOT_ATTEMPTS} attempts")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Surface on the next websocket message, which triggers a clean reconnect.
            self._snapshot_error = exc
