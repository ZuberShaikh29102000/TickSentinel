"""Pipeline: applies Ticks to local order books and summarizes them for storage.

Feed handlers call `on_tick` *synchronously*, with no queue in between. A queue
would add a context switch and allocation per tick and, if bounded, force a
choice between blocking the socket reader or dropping ticks *before* the book
(which would corrupt it). Applying a tick to the book takes microseconds, so
doing it inline is both the fastest and the correct option. The only place we
allow loss is the storage sink, which is bounded and drop-counting.

Every tick updates the in-memory books; storage only receives per-second
summaries (see aggregator.py). In Phase 2 the rule detectors hook in here, on
the same synchronous path: "the fast path detects".
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Protocol

from ticksentinel.common.models import Exchange, FeedEvent, Tick, TickKind
from ticksentinel.ingest.aggregator import MarketSecond, SecondAggregator
from ticksentinel.ingest.book import OrderBook

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BookTop:
    """Top-N view of one venue's book right after a tick was applied."""

    exchange: Exchange
    symbol: str
    kind: TickKind
    seq: int | None
    exch_ts_ns: int
    recv_ts_ns: int
    bids: list[tuple[float, float]]  # best first
    asks: list[tuple[float, float]]  # best first


class RowSink(Protocol):
    def add_market_second(self, row: MarketSecond) -> None: ...
    def add_event(self, event: FeedEvent) -> None: ...


class BookManager:
    def __init__(self, depth: int = 10) -> None:
        self.depth = depth
        self.books: dict[tuple[Exchange, str], OrderBook[float]] = {}

    def apply(self, tick: Tick) -> BookTop:
        book = self.books.setdefault((tick.exchange, tick.symbol), OrderBook())
        if tick.kind is TickKind.BOOK_SNAPSHOT:
            book.clear()
        book.update(
            ((lv.price, lv.qty) for lv in tick.bids), ((lv.price, lv.qty) for lv in tick.asks)
        )
        return BookTop(
            exchange=tick.exchange,
            symbol=tick.symbol,
            kind=tick.kind,
            seq=tick.seq,
            exch_ts_ns=tick.exch_ts_ns,
            recv_ts_ns=tick.recv_ts_ns,
            bids=book.top_bids(self.depth),
            asks=book.top_asks(self.depth),
        )


class Pipeline:
    def __init__(self, sink: RowSink | None = None, depth: int = 10) -> None:
        self.books = BookManager(depth)
        self.sink = sink
        self.aggregator = SecondAggregator(self._emit_second)
        self.ticks = Counter[Exchange]()
        self.events = Counter[str]()
        self.last_top: dict[tuple[Exchange, str], BookTop] = {}

    def on_tick(self, tick: Tick) -> None:
        self.ticks[tick.exchange] += 1
        if tick.kind is TickKind.TRADE:
            self.aggregator.on_trades(tick)
            return
        top = self.books.apply(tick)
        key = (tick.exchange, tick.symbol)
        self.last_top[key] = top
        self.aggregator.on_book(
            key,
            tick.recv_ts_ns,
            top.bids[0] if top.bids else None,
            top.asks[0] if top.asks else None,
        )

    def on_event(self, event: FeedEvent) -> None:
        self.events[f"{event.exchange}:{event.event_type}"] += 1
        log.info(
            "feed event %s %s", event.exchange, event.event_type, extra={"detail": event.detail}
        )
        if self.sink is not None:
            self.sink.add_event(event)

    def flush_seconds(self, now_ns: int) -> None:
        self.aggregator.flush(now_ns)

    def _emit_second(self, row: MarketSecond) -> None:
        if self.sink is not None:
            self.sink.add_market_second(row)
