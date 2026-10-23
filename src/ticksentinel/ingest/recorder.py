"""Raw message recorder: exact exchange bytes + receive timestamp, gzip-compressed.

The recording is the ground truth that everything else can be rebuilt from:
test fixtures, ML training data (Phase 3), and the UDP multicast replay (Phase 6).
Storing the raw text (not our parsed Ticks) means a bug in a normalizer can be
fixed and the data re-parsed, instead of being baked into the archive.

Line format: "<recv_ns>\t<source>\t<raw>\n", source = "ws" | "rest".
Compact exchange JSON never contains literal tabs or newlines (JSON escapes them
inside strings), so tab-separation is unambiguous and much cheaper than JSON-wrapping.
"""

from __future__ import annotations

import asyncio
import gzip
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

from ticksentinel.common.models import Exchange
from ticksentinel.ingest.base import RawHandler


@dataclass(frozen=True, slots=True)
class RecordedMessage:
    recv_ns: int
    source: str
    raw: str


class RawRecorder:
    """Buffers lines in memory on the hot path; a background task compresses and
    writes them in a worker thread so disk IO never stalls the event loop."""

    def __init__(
        self, directory: Path, exchanges: list[Exchange], flush_interval_s: float = 1.0
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.paths = {ex: directory / f"{ex.value}-{stamp}.tsv.gz" for ex in exchanges}
        # Long-lived handles (closed in close()), so no `with` block.
        self._files: dict[Exchange, IO[str]] = {
            ex: gzip.open(path, "at", encoding="utf-8")  # noqa: SIM115
            for ex, path in self.paths.items()
        }
        self._pending: dict[Exchange, list[str]] = {ex: [] for ex in exchanges}
        self._flush_interval_s = flush_interval_s
        self.lines_written = 0

    def appender(self, exchange: Exchange) -> RawHandler:
        pending = self._pending

        def append(source: str, raw: str, recv_ns: int) -> None:
            pending[exchange].append(f"{recv_ns}\t{source}\t{raw}\n")

        return append

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            await self.flush()

    async def flush(self) -> None:
        # Swap lists on the event loop thread (no lock needed: the loop is single
        # threaded), then do the slow gzip+disk work in a worker thread.
        batches: dict[Exchange, list[str]] = {}
        for ex, lines in self._pending.items():
            if lines:
                batches[ex], self._pending[ex] = lines, []
        if batches:
            await asyncio.to_thread(self._write, batches)

    def _write(self, batches: dict[Exchange, list[str]]) -> None:
        for ex, lines in batches.items():
            self._files[ex].writelines(lines)
            self._files[ex].flush()
            self.lines_written += len(lines)

    async def close(self) -> None:
        await self.flush()
        for f in self._files.values():
            f.close()


def read_recording(path: Path) -> Iterator[RecordedMessage]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            recv_ns, source, raw = line.rstrip("\n").split("\t", 2)
            yield RecordedMessage(int(recv_ns), source, raw)
