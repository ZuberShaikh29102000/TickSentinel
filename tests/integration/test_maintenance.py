"""Rollup, retention and the storage guard, against real Postgres 17."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from ticksentinel.storage.db import Database
from ticksentinel.storage.maintenance import (
    MaintenanceState,
    RetentionPolicy,
    rollup_minutes,
    run_maintenance,
)

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 20, 12, 0, 30, tzinfo=UTC)  # 30 s into the 12:00 minute
MINUTE = timedelta(minutes=1)
ALIGNED = NOW.replace(second=0)  # 60 rows from here fill exactly one minute


async def insert_seconds(
    conn: asyncpg.Connection,
    start: datetime,
    count: int,
    *,
    exchange: str = "kraken",
    bid: float = 100.0,
    spread: float = 1.0,
) -> None:
    rows = [
        (
            start + timedelta(seconds=i),
            exchange,
            "BTC-USD",
            bid + i,
            1.0,
            bid + i + spread,
            1.0,
            2,
            1,
            0.5,
            bid + i,
        )
        for i in range(count)
    ]
    await conn.executemany(
        "INSERT INTO market_1s VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", rows
    )


def acquire(db: Database) -> Any:
    assert db.pool is not None
    return db.pool.acquire()


async def test_rollup_produces_correct_minute_bars(db: Database) -> None:
    async with acquire(db) as conn:
        await insert_seconds(conn, NOW.replace(second=0) - 2 * MINUTE, 120)  # 11:58, 11:59 complete
        await insert_seconds(conn, NOW.replace(second=0), 30)  # 12:00 still filling
        rolled = await rollup_minutes(conn, NOW, timedelta(hours=1))
        bars = await conn.fetch("SELECT * FROM market_1m ORDER BY time")

    assert rolled == 2
    assert [b["time"] for b in bars] == [
        NOW.replace(second=0) - 2 * MINUTE,
        NOW.replace(second=0) - MINUTE,
    ]
    first = bars[0]
    # bids 100..159 with a 1.0 spread: mid = bid + 0.5
    assert (first["mid_open"], first["mid_close"]) == (100.5, 159.5)
    assert (first["mid_low"], first["mid_high"]) == (100.5, 159.5)
    assert (first["spread_avg"], first["spread_max"]) == (1.0, 1.0)
    assert (first["book_updates"], first["trade_count"], first["seconds_present"]) == (120, 60, 60)
    assert first["trade_volume"] == pytest.approx(30.0)


async def test_rollup_flags_minutes_with_missing_seconds(db: Database) -> None:
    async with acquire(db) as conn:
        await insert_seconds(conn, NOW.replace(second=0) - MINUTE, 45)  # 15 s of the minute missing
        await rollup_minutes(conn, NOW, timedelta(hours=1))
        present = await conn.fetchval("SELECT seconds_present FROM market_1m")
    assert present == 45


async def test_late_rows_are_picked_up_by_the_next_rollup(db: Database) -> None:
    """Rows buffered while the DB was unreachable arrive after their minute was rolled up."""
    minute = NOW.replace(second=0) - 2 * MINUTE
    async with acquire(db) as conn:
        await insert_seconds(conn, minute, 30)
        await rollup_minutes(conn, NOW, timedelta(hours=1))
        await insert_seconds(conn, minute + timedelta(seconds=30), 30, bid=130.0)  # late half
        await rollup_minutes(conn, NOW, timedelta(hours=1))
        bar = await conn.fetchrow("SELECT * FROM market_1m WHERE time = $1", minute)
    assert bar["seconds_present"] == 60 and bar["mid_close"] == 159.5


async def test_retention_keeps_7_days_of_seconds_and_30_days_of_minutes(db: Database) -> None:
    policy = RetentionPolicy()
    async with acquire(db) as conn:
        await insert_seconds(conn, ALIGNED - timedelta(days=8), 60)  # too old for market_1s
        await insert_seconds(conn, ALIGNED - timedelta(days=6), 60)  # kept
        await insert_seconds(conn, ALIGNED - timedelta(days=31), 60)  # too old for both tables
        result = await run_maintenance(conn, NOW, policy, MaintenanceState())
        seconds_left = await conn.fetchval("SELECT count(*) FROM market_1s")
        minute_times = [r["time"] for r in await conn.fetch("SELECT time FROM market_1m")]

    assert seconds_left == 60  # only the 6-day-old minute survives in market_1s
    assert result["deleted_1s"] == 120
    # The 8-day-old data was rolled up BEFORE its seconds were deleted, so the
    # 30-day history still has it; only the 31-day-old minute is gone.
    assert len(minute_times) == 2
    assert min(minute_times) > NOW - timedelta(days=30)


async def test_guard_trims_one_day_and_does_not_keep_deleting(db: Database) -> None:
    """DELETE does not shrink Postgres files, so the size stays above the limit
    after a trim. The guard must not read that as 'still too big, delete more',
    or it would wipe the whole history."""
    tiny = RetentionPolicy(guard_bytes=1)  # every real database is "over" this
    state = MaintenanceState()
    async with acquire(db) as conn:
        for days_ago in (6.5, 5.5, 1.5, 0.5):
            await insert_seconds(conn, NOW - timedelta(days=days_ago), 60)

        first = await run_maintenance(conn, NOW, tiny, state)
        assert first["guard"]["keep_1s_days"] == 6.0
        assert await conn.fetchval("SELECT count(*) FROM market_1s") == 180  # 6.5-day-old gone

        second = await run_maintenance(conn, NOW, tiny, state)  # size did not grow
        assert "guard" not in second
        assert await conn.fetchval("SELECT count(*) FROM market_1s") == 180

        events = await conn.fetch("SELECT event_type, detail FROM system_events")
    assert [e["event_type"] for e in events] == ["storage_guard"]


async def test_guard_never_trims_below_one_day(db: Database) -> None:
    policy = RetentionPolicy(guard_bytes=1)
    state = MaintenanceState(effective_keep_1s=timedelta(days=1))
    async with acquire(db) as conn:
        await insert_seconds(conn, NOW - timedelta(hours=12), 60)
        result = await run_maintenance(conn, NOW, policy, state)
        left = await conn.fetchval("SELECT count(*) FROM market_1s")
    assert "guard" not in result and left == 60
