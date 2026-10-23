"""Run TickSentinel: `python -m ticksentinel.ingest [--duration S] [--no-db] ...`.

Production (Render) runs this with --no-record; locally it also records raw
messages for tests, ML training data and replay benchmarks.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import time
from pathlib import Path

from ticksentinel.common.clock import wall_clock_ns
from ticksentinel.common.config import get_settings
from ticksentinel.common.logging import setup_logging
from ticksentinel.common.models import Exchange
from ticksentinel.ingest.base import FeedClient
from ticksentinel.ingest.binance import BinanceClient
from ticksentinel.ingest.coinbase import CoinbaseClient
from ticksentinel.ingest.kraken import KrakenClient
from ticksentinel.ingest.pipeline import BookTop, Pipeline
from ticksentinel.ingest.recorder import RawRecorder
from ticksentinel.ops.health import build_health, serve
from ticksentinel.storage.db import Database
from ticksentinel.storage.maintenance import Maintainer
from ticksentinel.storage.writer import DbWriter

log = logging.getLogger("ticksentinel.ingest")

CLIENTS: dict[Exchange, type[FeedClient]] = {
    Exchange.BINANCE: BinanceClient,
    Exchange.COINBASE: CoinbaseClient,
    Exchange.KRAKEN: KrakenClient,
}


def parse_args(default_port: int) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    p.add_argument("--no-db", action="store_true", help="don't write to the database")
    p.add_argument("--record-dir", type=Path, default=None, help="raw recording directory")
    p.add_argument("--no-record", action="store_true", help="don't write raw recordings")
    p.add_argument("--port", type=int, default=default_port, help="HTTP port for /health")
    p.add_argument("--no-http", action="store_true", help="don't serve /health")
    p.add_argument(
        "--exchanges",
        default=",".join(e.value for e in Exchange),
        help="comma-separated subset, e.g. binance,kraken",
    )
    p.add_argument("--stats-every", type=float, default=60.0)
    return p.parse_args()


def _top_of_book(top: BookTop) -> str:
    bid = top.bids[0][0] if top.bids else None
    ask = top.asks[0][0] if top.asks else None
    return f"{bid} / {ask}"


async def log_stats(
    pipeline: Pipeline, clients: list[FeedClient], writer: DbWriter | None, every: float
) -> None:
    while True:
        await asyncio.sleep(every)
        tops = {ex.value: _top_of_book(t) for (ex, _), t in pipeline.last_top.items()}
        log.info(
            "ticks=%s msgs=%s top=%s written=%s dropped=%s pending=%s",
            {ex.value: n for ex, n in pipeline.ticks.items()},
            {c.exchange.value: c.messages_received for c in clients},
            tops,
            dict(writer.written) if writer else {},
            dict(writer.dropped) if writer else {},
            writer.pending() if writer else 0,
        )


async def emit_seconds(pipeline: Pipeline) -> None:
    """Close each second shortly after it ends, so per-second rows keep flowing even
    for a feed that has gone completely silent."""
    while True:
        await asyncio.sleep(1.0 - (wall_clock_ns() % 1_000_000_000) / 1e9 + 0.05)
        pipeline.flush_seconds(wall_clock_ns())


def _stop_on_sigterm() -> None:
    # Render sends SIGTERM before stopping or redeploying the instance. Turning it
    # into task cancellation runs the `finally` cleanup that writes buffered rows.
    task = asyncio.current_task()
    if task is None:
        return
    with contextlib.suppress(NotImplementedError):  # no add_signal_handler on Windows
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)


async def main() -> None:
    settings = get_settings()
    args = parse_args(settings.port)
    setup_logging(settings.log_dir)
    _stop_on_sigterm()
    exchanges = [Exchange(e.strip()) for e in args.exchanges.split(",")]
    started = time.monotonic()

    db = None if args.no_db else Database(settings.postgres_dsn)
    writer = DbWriter(db) if db is not None else None
    maintainer = Maintainer(db) if db is not None else None

    recorder = None
    if not args.no_record:
        recorder = RawRecorder(args.record_dir or settings.record_dir, exchanges)
        log.info("recording raw messages to %s", [str(p) for p in recorder.paths.values()])

    pipeline = Pipeline(writer, depth=settings.book_depth_stored)
    clients = [
        CLIENTS[ex](
            pipeline.on_tick, pipeline.on_event, recorder.appender(ex) if recorder else None
        )
        for ex in exchanges
    ]

    try:
        async with asyncio.timeout(args.duration), asyncio.TaskGroup() as tg:
            for client in clients:
                tg.create_task(client.run(), name=f"feed-{client.exchange.value}")
            tg.create_task(emit_seconds(pipeline), name="seconds")
            if writer is not None and maintainer is not None:
                tg.create_task(writer.run(), name="db-writer")
                tg.create_task(maintainer.run(), name="maintenance")
            if recorder is not None:
                tg.create_task(recorder.run(), name="recorder")
            if not args.no_http:
                status = lambda: build_health(clients, writer, maintainer, started)  # noqa: E731
                tg.create_task(serve(status, args.port), name="http")
                log.info("health endpoint on http://0.0.0.0:%d/health", args.port)
            tg.create_task(log_stats(pipeline, clients, writer, args.stats_every), name="stats")
    except TimeoutError:
        log.info("duration reached, shutting down")
    finally:
        # Drain what we already have so a clean stop loses as little as possible.
        pipeline.flush_seconds(wall_clock_ns())
        if writer is not None:
            await writer.flush_all()
            log.info("db rows written=%s dropped=%s", dict(writer.written), dict(writer.dropped))
        if db is not None:
            await db.close()
        if recorder is not None:
            await recorder.close()
            log.info("raw lines recorded=%d", recorder.lines_written)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())
