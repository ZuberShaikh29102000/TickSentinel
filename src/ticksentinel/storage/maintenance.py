"""Keeps the database inside its storage budget, forever, without supervision.

Supabase free has 500 MB and switches the database to read-only when exceeded,
which would silently stop the live system from storing anything. So:

  every 5 minutes  roll complete minutes of market_1s up into market_1m
  every hour       delete market_1s older than 7 days, market_1m older than 30 days
  every run        storage guard: above 400 MB, shorten market_1s retention by a day

This runs in our own process (not Supabase's pg_cron) so it behaves identically
on the local Postgres container, where the tests exercise it.

Why the guard needs care: DELETE does not shrink Postgres files. Freed space is
only marked reusable (by VACUUM) and filled by future rows, so the database size
stays flat after a cleanup instead of dropping. A naive "while size > limit:
delete more" loop would therefore delete everything. Instead the guard trims
one day at a time and trips again only if the size has kept GROWING by
GUARD_REARM_BYTES since its last trip, i.e. only if the previous trim did not
stop the growth.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import orjson

from ticksentinel.storage.db import RETRYABLE_ERRORS, Database

log = logging.getLogger(__name__)

MB = 1024 * 1024
GUARD_REARM_BYTES = 10 * MB


@dataclass
class RetentionPolicy:
    keep_1s: timedelta = timedelta(days=7)
    keep_1m: timedelta = timedelta(days=30)
    min_keep_1s: timedelta = timedelta(days=1)  # the guard never trims below this
    guard_bytes: int = 400 * MB
    rollup_every: timedelta = timedelta(minutes=5)
    retention_every: timedelta = timedelta(hours=1)
    # Recompute this much recent history on each rollup: rows written late (e.g.
    # buffered while the database was unreachable) then still reach market_1m.
    rollup_lookback: timedelta = timedelta(hours=1)


@dataclass
class MaintenanceState:
    effective_keep_1s: timedelta | None = None  # shortened by the guard
    guard_tripped_at_bytes: int | None = None
    last_retention: datetime | None = None
    runs: int = 0
    last_result: dict[str, Any] = field(default_factory=dict)


ROLLUP_SQL = """
INSERT INTO market_1m (time, exchange, symbol, mid_open, mid_high, mid_low, mid_close,
                       spread_avg, spread_max, book_updates, trade_count, trade_volume,
                       seconds_present)
SELECT date_trunc('minute', time) AS minute, exchange, symbol,
       (array_agg(mid ORDER BY time)      FILTER (WHERE mid IS NOT NULL))[1],
       max(mid), min(mid),
       (array_agg(mid ORDER BY time DESC) FILTER (WHERE mid IS NOT NULL))[1],
       avg(spread), max(spread),
       sum(book_updates), sum(trade_count), sum(trade_volume), count(*)
FROM (
    SELECT time, exchange, symbol, book_updates, trade_count, trade_volume,
           (best_bid + best_ask) / 2 AS mid, best_ask - best_bid AS spread
    FROM market_1s
    WHERE time >= $1 AND time < $2
) s
GROUP BY 1, 2, 3
ON CONFLICT (time, exchange, symbol) DO UPDATE SET
    mid_open = EXCLUDED.mid_open, mid_high = EXCLUDED.mid_high,
    mid_low = EXCLUDED.mid_low, mid_close = EXCLUDED.mid_close,
    spread_avg = EXCLUDED.spread_avg, spread_max = EXCLUDED.spread_max,
    book_updates = EXCLUDED.book_updates, trade_count = EXCLUDED.trade_count,
    trade_volume = EXCLUDED.trade_volume, seconds_present = EXCLUDED.seconds_present
"""


async def rollup_minutes(conn: asyncpg.Connection, now: datetime, lookback: timedelta) -> int:
    """Roll every COMPLETE minute since (last rolled minute - lookback) into market_1m."""
    last = await conn.fetchval("SELECT max(time) FROM market_1m")
    start = (last - lookback) if last is not None else datetime.min.replace(tzinfo=UTC)
    end = now.replace(second=0, microsecond=0)  # the current minute is still filling
    status = await conn.execute(ROLLUP_SQL, start, end)
    return int(status.split()[-1])  # "INSERT 0 <n>"


async def delete_older_than(conn: asyncpg.Connection, table: str, cutoff: datetime) -> int:
    """One range delete on the time-first primary key."""
    assert table in ("market_1s", "market_1m")  # never interpolate untrusted names
    status = await conn.execute(f"DELETE FROM {table} WHERE time < $1", cutoff)
    return int(status.split()[-1])


async def database_size(conn: asyncpg.Connection) -> int:
    size: int = await conn.fetchval("SELECT pg_database_size(current_database())")
    return size


async def record_system_event(
    conn: asyncpg.Connection, now: datetime, event_type: str, detail: dict[str, Any]
) -> None:
    await conn.execute(
        "INSERT INTO system_events (time, event_type, detail) VALUES ($1, $2, $3)",
        now,
        event_type,
        orjson.dumps(detail).decode(),
    )


async def run_maintenance(
    conn: asyncpg.Connection,
    now: datetime,
    policy: RetentionPolicy,
    state: MaintenanceState,
) -> dict[str, Any]:
    keep_1s = state.effective_keep_1s or policy.keep_1s
    result: dict[str, Any] = {
        "rolled_up_minutes": await rollup_minutes(conn, now, policy.rollup_lookback)
    }

    if state.last_retention is None or now - state.last_retention >= policy.retention_every:
        # Roll up first (above), so nothing is deleted before it reaches market_1m.
        result["deleted_1s"] = await delete_older_than(conn, "market_1s", now - keep_1s)
        result["deleted_1m"] = await delete_older_than(conn, "market_1m", now - policy.keep_1m)
        state.last_retention = now

    size = await database_size(conn)
    result["db_bytes"] = size
    if _guard_should_trip(size, policy, state) and keep_1s > policy.min_keep_1s:
        keep_1s = max(keep_1s - timedelta(days=1), policy.min_keep_1s)
        state.effective_keep_1s = keep_1s
        state.guard_tripped_at_bytes = size
        deleted = await delete_older_than(conn, "market_1s", now - keep_1s)
        # Plain VACUUM (no lock on writes) marks the freed space reusable at once.
        await conn.execute("VACUUM market_1s")
        detail = {"db_bytes": size, "keep_1s_days": keep_1s / timedelta(days=1), "deleted": deleted}
        await record_system_event(conn, now, "storage_guard", detail)
        log.warning("storage guard tripped: %s", detail)
        result["guard"] = detail

    result["keep_1s_days"] = keep_1s / timedelta(days=1)
    state.runs += 1
    state.last_result = result
    return result


def _guard_should_trip(size: int, policy: RetentionPolicy, state: MaintenanceState) -> bool:
    if size <= policy.guard_bytes:
        return False
    if state.guard_tripped_at_bytes is None:
        return True
    return size >= state.guard_tripped_at_bytes + GUARD_REARM_BYTES


class Maintainer:
    """Runs run_maintenance() on a timer. Like the writer, it simply skips a round
    when the database is unreachable; it never stops the rest of the system."""

    def __init__(
        self,
        db: Database,
        policy: RetentionPolicy | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.db = db
        self.policy = policy or RetentionPolicy()
        self.state = MaintenanceState()
        self._clock = clock
        self.last_error: str | None = None

    async def run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(self.policy.rollup_every.total_seconds())

    async def run_once(self) -> None:
        pool = await self.db.ensure()
        if pool is None:
            return
        try:
            async with pool.acquire() as conn:
                result = await run_maintenance(conn, self._clock(), self.policy, self.state)
            self.last_error = None
            log.info("maintenance: %s", result)
        except RETRYABLE_ERRORS as exc:
            await self.db.invalidate(exc)
        except asyncpg.PostgresError as exc:
            self.last_error = repr(exc)
            log.exception("maintenance failed")
