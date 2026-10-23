"""Kraken spot WebSocket v2 feed handler (book + trade).

Docs: https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book
      https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2

Integrity model: Kraken's v2 public feed has NO sequence numbers. Instead each
book message carries a CRC32 checksum of the top 10 levels *after* the update is
applied. Recomputing it proves our book matches theirs exactly; a mismatch means
a lost or corrupted update (or a bug in our book code), so we resubscribe.

The checksum is computed over the exact decimal text of prices and quantities,
so this handler parses JSON with Decimal and keeps a Decimal copy of the book.
"""

from __future__ import annotations

import json
import logging
import zlib
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import orjson
from websockets.asyncio.client import ClientConnection

from ticksentinel.common.clock import rfc3339_to_ns
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
from ticksentinel.ingest.book import OrderBook

log = logging.getLogger(__name__)

WS_URL = "wss://ws.kraken.com/v2"
CHECKSUM_LEVELS = 10  # fixed by Kraken regardless of subscription depth


def _checksum_token(value: Decimal) -> str:
    # format(.., "f") not str(): str(Decimal("0.00000051")) is "5.1E-7".
    return format(value, "f").replace(".", "").lstrip("0")


def kraken_checksum(
    asks: Sequence[tuple[Decimal, Decimal]], bids: Sequence[tuple[Decimal, Decimal]]
) -> int:
    """CRC32 over top-10 asks (low->high) then top-10 bids (high->low)."""
    parts = [
        _checksum_token(price) + _checksum_token(qty)
        for price, qty in [*asks[:CHECKSUM_LEVELS], *bids[:CHECKSUM_LEVELS]]
    ]
    return zlib.crc32("".join(parts).encode())


def _decimal_levels(entries: list[dict[str, Decimal]]) -> list[tuple[Decimal, Decimal]]:
    return [(e["price"], e["qty"]) for e in entries]


def _float_levels(entries: list[dict[str, Decimal]]) -> tuple[Level, ...]:
    return tuple(Level(float(e["price"]), float(e["qty"])) for e in entries)


class KrakenClient(FeedClient):
    exchange = Exchange.KRAKEN

    def __init__(
        self,
        *args: Any,
        pair: str = "BTC/USD",
        canonical_symbol: str = "BTC-USD",
        depth: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.pair = pair
        self.canonical_symbol = canonical_symbol
        self.depth = depth
        self.book: OrderBook[Decimal] = OrderBook()
        self.local_seq = 0
        self.checksums_verified = 0

    @property
    def url(self) -> str:
        return WS_URL

    async def subscribe(self, ws: ClientConnection) -> None:
        book = {"channel": "book", "symbol": [self.pair], "depth": self.depth, "snapshot": True}
        # snapshot=False: the trade snapshot is historical trades, not live data.
        trade = {"channel": "trade", "symbol": [self.pair], "snapshot": False}
        for params in (book, trade):
            await ws.send(orjson.dumps({"method": "subscribe", "params": params}).decode())

    def reset_state(self) -> None:
        self.book.clear()
        self.local_seq = 0

    def on_message(self, raw: str, recv_ns: int) -> None:
        # stdlib json (not orjson) because only it can decode floats straight to Decimal,
        # preserving the exact text ("0.00100000") the checksum is computed over.
        msg = json.loads(raw, parse_float=Decimal)
        channel = msg.get("channel")
        if channel == "book":
            for entry in msg["data"]:
                self._on_book(entry, msg["type"], recv_ns)
        elif channel == "trade":
            self._on_trades(msg["data"], recv_ns)
        elif msg.get("method") == "subscribe":
            if not msg.get("success"):
                self.emit_event(FeedEventType.ERROR, {"subscribe_error": msg.get("error")})
                raise ResyncRequired(f"subscribe failed: {msg.get('error')}")
            self.emit_event(FeedEventType.SUBSCRIBED, {"result": _jsonable(msg["result"])})
        # "heartbeat" and "status" need no action.

    def _on_book(self, entry: dict[str, Any], msg_type: str, recv_ns: int) -> None:
        if msg_type == "snapshot":
            self.book.clear()
        bids, asks = _decimal_levels(entry["bids"]), _decimal_levels(entry["asks"])
        self.book.update(bids, asks)
        # Kraken never sends qty=0 for levels that scroll past our depth; trim them
        # ourselves or stale levels would corrupt both the book and the checksum.
        evicted_bids, evicted_asks = self.book.truncate(self.depth)
        self._verify_checksum(int(entry["checksum"]))
        # Emit the evictions as explicit removals, so the Tick alone fully describes the
        # change and any downstream book (pipeline, C++, replay) stays in sync. Without
        # this, stale levels lingered downstream and once surfaced as a crossed book.
        bid_removals = tuple(Level(float(p), 0.0) for p in evicted_bids)
        ask_removals = tuple(Level(float(p), 0.0) for p in evicted_asks)

        self.local_seq += 1
        self.emit_tick(
            Tick(
                exchange=self.exchange,
                symbol=self.canonical_symbol,
                kind=TickKind.BOOK_SNAPSHOT if msg_type == "snapshot" else TickKind.BOOK_UPDATE,
                # No exchange sequence exists; this local counter orders our own
                # output but cannot reveal exchange-side loss (the checksum does that).
                seq=self.local_seq,
                exch_ts_ns=rfc3339_to_ns(entry["timestamp"]),
                recv_ts_ns=recv_ns,
                bids=_float_levels(entry["bids"]) + bid_removals,
                asks=_float_levels(entry["asks"]) + ask_removals,
            )
        )

    def _verify_checksum(self, expected: int) -> None:
        computed = kraken_checksum(
            self.book.top_asks(CHECKSUM_LEVELS), self.book.top_bids(CHECKSUM_LEVELS)
        )
        if computed != expected:
            self.emit_event(
                FeedEventType.CHECKSUM_MISMATCH, {"expected": expected, "computed": computed}
            )
            raise ResyncRequired(f"checksum mismatch: expected {expected}, computed {computed}")
        self.checksums_verified += 1

    def _on_trades(self, data: list[dict[str, Any]], recv_ns: int) -> None:
        trades = tuple(
            Trade(
                trade_id=str(t["trade_id"]),
                price=float(t["price"]),
                qty=float(t["qty"]),
                # Kraken reports the taker's side, which is already our convention.
                side=Side(t["side"]),
                exch_ts_ns=rfc3339_to_ns(t["timestamp"]),
            )
            for t in data
        )
        if not trades:
            return
        self.emit_tick(
            Tick(
                exchange=self.exchange,
                symbol=self.canonical_symbol,
                kind=TickKind.TRADE,
                seq=None,
                exch_ts_ns=max(t.exch_ts_ns for t in trades),
                recv_ts_ns=recv_ns,
                trades=trades,
            )
        )


def _jsonable(obj: Any) -> Any:
    """Decimals from parse_float aren't JSON-serializable downstream; convert for event detail."""
    return orjson.loads(json.dumps(obj, default=str))
