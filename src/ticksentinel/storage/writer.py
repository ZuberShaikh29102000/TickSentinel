"""Batched, non-blocking writer from the pipeline into Postgres.

Design: the hot path (`add_*`) only appends to a bounded in-memory buffer; a
background task converts and inserts batches. If the database is slow or down,
rows wait in the buffer (about 9 hours of market_1s at 3 rows/s) and are
written when it comes back. Only if the buffer is full do we DROP new rows and
count them; we never block the feed handlers. Ingestion and detection must
never wait on storage.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter, deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import orjson

from ticksentinel.common.models import FeedEvent
from ticksentinel.ingest.aggregator import MarketSecond
from ticksentinel.storage.db import RETRYABLE_ERRORS, Database

log = logging.getLogger(__name__)

# ON CONFLICT DO NOTHING: after a restart, the first second written by the new
# process can overlap the last second written by the old one. A plain COPY would
# fail the whole batch on that duplicate key; at ~3 rows/s, INSERT is cheap enough.
INSERT_MARKET_1S = """
    INSERT INTO market_1s (time, exchange, symbol, best_bid, best_bid_qty, best_ask,
                           best_ask_qty, book_updates, trade_count, trade_volume, last_trade_px)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (time, exchange, symbol) DO NOTHING
"""
INSERT_FEED_EVENT = """
    INSERT INTO feed_events (time, exchange, event_type, detail) VALUES ($1, $2, $3, $4)
"""

MAX_PENDING_SECONDS = 100_000  # ~9 h of 3 feeds; ~20 MB of memory at worst
MAX_PENDING_EVENTS = 50_000  # events are rare; the cap only guards a pathological flood


def ns_to_datetime(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, tz=UTC)


def market_second_record(row: MarketSecond) -> tuple[Any, ...]:
    return (
        datetime.fromtimestamp(row.second, tz=UTC),
        row.exchange.value,
        row.symbol,
        row.best_bid,
        row.best_bid_qty,
        row.best_ask,
        row.best_ask_qty,
        row.book_updates,
        row.trade_count,
        row.trade_volume,
        row.last_trade_px,
    )


def event_record(event: FeedEvent) -> tuple[Any, ...]:
    return (
        ns_to_datetime(event.ts_ns),
        event.exchange.value,
        event.event_type.value,
        orjson.dumps(event.detail, default=str).decode(),
    )


class DbWriter:
    def __init__(
        self,
        db: Database | None,
        *,
        max_pending_seconds: int = MAX_PENDING_SECONDS,
        max_pending_events: int = MAX_PENDING_EVENTS,
        batch_size: int = 1_000,
        flush_interval_s: float = 1.0,
    ) -> None:
        self.db = db
        self._max_seconds = max_pending_seconds
        self._max_events = max_pending_events
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._seconds: deque[MarketSecond] = deque()
        self._events: deque[FeedEvent] = deque()
        self.written = Counter[str]()
        self.dropped = Counter[str]()

    # ---- hot path: O(1), never blocks ----------------------------------------

    def pending(self) -> int:
        return len(self._seconds) + len(self._events)

    def add_market_second(self, row: MarketSecond) -> None:
        if len(self._seconds) >= self._max_seconds:
            self.dropped["market_1s"] += 1
            return
        self._seconds.append(row)

    def add_event(self, event: FeedEvent) -> None:
        # Feed events are the evidence trail for incidents, so they get their own,
        # separate budget: a flood of market rows can never push them out.
        if len(self._events) >= self._max_events:
            self.dropped["feed_events"] += 1
            return
        self._events.append(event)

    # ---- background ------------------------------------------------------------

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            await self.flush()

    async def flush(self) -> None:
        """Write one batch per table. Leaves rows buffered if the DB is unreachable."""
        if self.db is None or not self.pending():
            return
        pool = await self.db.ensure()
        if pool is None:
            return
        await self._write(pool, "market_1s", INSERT_MARKET_1S, self._seconds, market_second_record)
        await self._write(pool, "feed_events", INSERT_FEED_EVENT, self._events, event_record)

    async def flush_all(self, max_failed_rounds: int = 3) -> None:
        """Best-effort drain on shutdown; gives up after a few rounds without progress."""
        failed = 0
        while self.pending() and failed < max_failed_rounds:
            before = self.pending()
            await self.flush()
            if self.pending() >= before:
                failed += 1
        if self.pending():
            log.warning("shutdown: %d rows could not be written", self.pending())

    async def _write(
        self,
        pool: asyncpg.Pool,
        table: str,
        sql: str,
        buf: deque[Any],
        to_record: Callable[[Any], tuple[Any, ...]],
    ) -> None:
        batch = [buf.popleft() for _ in range(min(self._batch_size, len(buf)))]
        if not batch:
            return
        try:
            async with pool.acquire() as conn:
                await conn.executemany(sql, [to_record(item) for item in batch])
        except RETRYABLE_ERRORS as exc:
            # Connectivity problem: put the batch back (oldest first) and reconnect later.
            buf.extendleft(reversed(batch))
            assert self.db is not None
            await self.db.invalidate(exc)
            return
        except asyncpg.PostgresError as exc:
            # Data or state problem (e.g. database read-only): retrying won't help.
            self.dropped[table] += len(batch)
            log.error("insert into %s failed, dropped %d rows: %r", table, len(batch), exc)
            return
        self.written[table] += len(batch)
