"""DbWriter and Database against real Postgres 17 (the same major version as Supabase)."""

from __future__ import annotations

import pytest

from ticksentinel.common.models import Exchange, FeedEvent, FeedEventType
from ticksentinel.ingest.aggregator import MarketSecond
from ticksentinel.storage.db import Database
from ticksentinel.storage.writer import DbWriter

pytestmark = pytest.mark.integration


def second(n: int, bid: float = 99.0) -> MarketSecond:
    return MarketSecond(n, Exchange.KRAKEN, "BTC-USD", bid, 1.0, 101.0, 2.0, 3, 1, 0.5, 100.0)


async def test_schema_is_created_on_first_connect(db: Database) -> None:
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        tables = {
            r["tablename"]
            for r in await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        }
    assert tables == {"market_1s", "market_1m", "feed_events", "system_events"}


async def test_schema_apply_is_idempotent(test_dsn: str, db: Database) -> None:
    # A second process (or a restart) runs the schema again against existing tables.
    again = Database(test_dsn)
    assert await again.ensure() is not None, again.last_error
    await again.close()


async def test_rows_round_trip(db: Database) -> None:
    writer = DbWriter(db)
    writer.add_market_second(second(1_791_825_729))
    writer.add_event(
        FeedEvent(
            Exchange.KRAKEN,
            FeedEventType.CHECKSUM_MISMATCH,
            1_791_825_729_123_456_789,
            {"expected": 1, "computed": 2},
        )
    )
    await writer.flush_all()

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM market_1s")
        event = await conn.fetchrow("SELECT * FROM feed_events")
    assert row["time"].timestamp() == 1_791_825_729
    assert (row["best_bid"], row["best_ask"], row["book_updates"]) == (99.0, 101.0, 3)
    assert event["event_type"] == "checksum_mismatch"
    assert writer.written == {"market_1s": 1, "feed_events": 1} and writer.dropped == {}


async def test_duplicate_second_after_restart_does_not_fail_the_batch(db: Database) -> None:
    """After a restart the new process can write a second the old one already wrote.
    That row is skipped; the rest of the batch must still be written."""
    old_process = DbWriter(db)
    old_process.add_market_second(second(100, bid=99.0))
    await old_process.flush_all()

    new_process = DbWriter(db)
    for n, bid in [(100, 98.0), (101, 98.5), (102, 98.7)]:
        new_process.add_market_second(second(n, bid))
    await new_process.flush_all()

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT time, best_bid FROM market_1s ORDER BY time")
    assert [(int(r["time"].timestamp()), r["best_bid"]) for r in rows] == [
        (100, 99.0),
        (101, 98.5),
        (102, 98.7),  # first writer wins second 100
    ]
    assert new_process.dropped == {}


async def test_unreachable_database_does_not_raise() -> None:
    # Port 1: nothing listens. ensure() must report, not raise, so ingestion survives.
    down = Database("postgresql://user:pw@127.0.0.1:1/none")
    assert await down.ensure() is None
    assert down.last_error is not None
    assert await down.ensure() is None  # inside backoff window: no new attempt, no raise
