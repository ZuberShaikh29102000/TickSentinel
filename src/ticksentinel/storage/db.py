"""Database connection that is allowed to be missing.

In a live system the database (Supabase) can be unreachable for minutes: network
blips, maintenance, a cold pooler. Ingestion and detection must keep running
regardless, so nothing waits on `connect()`: callers ask `ensure()` for a pool,
get None while the database is down, and try again later with backoff.
"""

from __future__ import annotations

import logging
import time
from importlib import resources

import asyncpg
from asyncpg import exceptions as pgerr

from ticksentinel.ingest.backoff import Backoff

log = logging.getLogger(__name__)

# Errors that mean "the database is unreachable or restarting": keep the data and
# retry later. Anything else (bad data, read-only database) is not fixed by
# waiting, so callers handle it differently.
RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    TimeoutError,
    asyncpg.InterfaceError,
    pgerr.PostgresConnectionError,
    pgerr.OperatorInterventionError,  # admin shutdown, "cannot connect now" during restart
    pgerr.TooManyConnectionsError,
)


def schema_sql() -> str:
    return resources.files("ticksentinel.storage").joinpath("schema.sql").read_text("utf-8")


class Database:
    def __init__(self, dsn: str, *, backoff: Backoff | None = None) -> None:
        self._dsn = dsn
        self._backoff = backoff or Backoff(base_s=1.0, cap_s=60.0)
        self._next_attempt = 0.0
        self.pool: asyncpg.Pool | None = None
        self.last_error: str | None = None
        self.connects = 0

    async def ensure(self) -> asyncpg.Pool | None:
        """The pool if connected; otherwise one connection attempt if the backoff
        allows it. Never raises for connectivity problems."""
        if self.pool is not None:
            return self.pool
        if time.monotonic() < self._next_attempt:
            return None
        pool: asyncpg.Pool | None = None
        try:
            pool = await asyncpg.create_pool(
                self._dsn, min_size=1, max_size=3, timeout=10, command_timeout=60
            )
            async with pool.acquire() as conn:
                await conn.execute(schema_sql())  # idempotent: CREATE ... IF NOT EXISTS
        except Exception as exc:
            # Broad on purpose: even a misconfiguration (wrong password, bad URL) must
            # not take ingestion down. It is logged loudly, shown on /health, retried.
            if pool is not None:
                pool.terminate()
            if not isinstance(exc, RETRYABLE_ERRORS):
                log.error("database connect failed (not a network error): %r", exc)
            self._fail(exc)
            return None
        self.pool = pool
        self.last_error = None
        self.connects += 1
        self._backoff.reset()
        log.info("database connected (connection #%d)", self.connects)
        return pool

    async def invalidate(self, exc: BaseException) -> None:
        """Drop a pool that just failed, so the next ensure() reconnects."""
        pool, self.pool = self.pool, None
        self._fail(exc)
        if pool is not None:
            pool.terminate()

    def _fail(self, exc: BaseException) -> None:
        delay = self._backoff.next_delay()
        self._next_attempt = time.monotonic() + delay
        self.last_error = f"{type(exc).__name__}: {exc}"
        log.warning("database unavailable (%s); retry in %.1fs", self.last_error, delay)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
