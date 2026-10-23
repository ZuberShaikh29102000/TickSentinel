"""DbWriter buffering, retry and drop behaviour, against a fake database."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ticksentinel.common.models import Exchange, FeedEvent, FeedEventType
from ticksentinel.ingest.aggregator import MarketSecond
from ticksentinel.storage.writer import DbWriter, event_record, market_second_record


def second(n: int) -> MarketSecond:
    return MarketSecond(n, Exchange.KRAKEN, "BTC-USD", 99.0, 1.0, 101.0, 2.0, 3, 1, 0.5, 100.0)


class FakeConn:
    def __init__(self, fail_with: BaseException | None) -> None:
        self.fail_with = fail_with
        self.rows: list[tuple[Any, ...]] = []

    async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.rows.extend(rows)


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeConn]:
        yield self.conn


class FakeDb:
    def __init__(self, fail_with: BaseException | None = None, up: bool = True) -> None:
        self.conn = FakeConn(fail_with)
        self.pool: FakePool | None = FakePool(self.conn) if up else None
        self.invalidated: list[BaseException] = []

    async def ensure(self) -> FakePool | None:
        return self.pool

    async def invalidate(self, exc: BaseException) -> None:
        self.invalidated.append(exc)


def make(db: FakeDb, **kw: Any) -> DbWriter:
    return DbWriter(db, **kw)  # type: ignore[arg-type]  # duck-typed fake


def test_market_second_record_layout() -> None:
    rec = market_second_record(second(1_791_825_729))
    assert rec[0] == datetime(2026, 10, 12, 17, 22, 9, tzinfo=UTC)
    assert rec[1:] == ("kraken", "BTC-USD", 99.0, 1.0, 101.0, 2.0, 3, 1, 0.5, 100.0)


def test_event_record_layout() -> None:
    ev = FeedEvent(Exchange.COINBASE, FeedEventType.SEQUENCE_GAP, 5, {"missed": 3})
    assert event_record(ev)[1:] == ("coinbase", "sequence_gap", '{"missed":3}')


async def test_rows_are_written_and_counted() -> None:
    db = FakeDb()
    writer = make(db)
    for n in range(3):
        writer.add_market_second(second(n))
    writer.add_event(FeedEvent(Exchange.KRAKEN, FeedEventType.CONNECTED, 1))
    await writer.flush()
    assert writer.written == {"market_1s": 3, "feed_events": 1}
    assert len(db.conn.rows) == 4 and writer.pending() == 0


async def test_database_down_keeps_rows_buffered() -> None:
    db = FakeDb(up=False)
    writer = make(db)
    writer.add_market_second(second(1))
    await writer.flush()
    assert writer.pending() == 1 and writer.written == {}


async def test_connection_failure_puts_batch_back_in_order() -> None:
    db = FakeDb(fail_with=ConnectionResetError("network blip"))
    writer = make(db)
    for n in range(5):
        writer.add_market_second(second(n))
    await writer.flush()
    assert writer.pending() == 5 and writer.dropped == {}
    assert len(db.invalidated) == 1  # the writer asks for a reconnect

    db.conn.fail_with = None  # database is back
    await writer.flush()
    assert [r[0].timestamp() for r in db.conn.rows] == [0, 1, 2, 3, 4]  # nothing lost or reordered


async def test_data_error_drops_batch_instead_of_retrying_forever() -> None:
    db = FakeDb(fail_with=asyncpg.exceptions.ReadOnlySQLTransactionError("read-only"))
    writer = make(db)
    writer.add_market_second(second(1))
    await writer.flush()
    assert writer.dropped["market_1s"] == 1 and writer.pending() == 0
    assert db.invalidated == []  # not a connectivity problem


def test_full_buffer_drops_and_counts_but_events_have_their_own_budget() -> None:
    writer = make(FakeDb(), max_pending_seconds=3, max_pending_events=2)
    for n in range(5):
        writer.add_market_second(second(n))
    assert writer.dropped["market_1s"] == 2
    ev = FeedEvent(Exchange.KRAKEN, FeedEventType.CHECKSUM_MISMATCH, 1)
    writer.add_event(ev)  # market rows filling up cannot push incident evidence out
    writer.add_event(ev)
    writer.add_event(ev)
    assert writer.dropped["feed_events"] == 1 and writer.pending() == 5


async def test_flush_all_gives_up_when_database_stays_down() -> None:
    writer = make(FakeDb(up=False))
    writer.add_market_second(second(1))
    await writer.flush_all()  # must return, not hang
    assert writer.pending() == 1
