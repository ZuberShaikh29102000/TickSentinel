"""High-resolution wall clock and timestamp parsing.

Why this exists: on Windows with Python < 3.13, time.time_ns() is backed by
GetSystemTimeAsFileTime, which only advances every ~1-15.6 ms. A receive
timestamp that coarse would make every latency number in this project noise.

Fix: read the wall clock once (the "anchor") and advance it with
perf_counter_ns, which has ~100 ns resolution. Trade-off: the absolute value is
only as accurate as the anchor (~1 ms on such systems), and perf_counter drifts
relative to NTP-disciplined time, so we re-anchor periodically. Intervals
between two readings are precise, which is what latency measurement needs.
Where time.time_ns() is already precise (Linux, Python >= 3.13), we use it directly.
"""

from __future__ import annotations

import calendar
import time
from datetime import datetime
from functools import lru_cache

_REANCHOR_EVERY_NS = 60 * 1_000_000_000


class AnchoredClock:
    def __init__(self) -> None:
        self._anchor()

    def _anchor(self) -> None:
        self._wall_anchor = time.time_ns()
        self._perf_anchor = time.perf_counter_ns()

    def now_ns(self) -> int:
        elapsed = time.perf_counter_ns() - self._perf_anchor
        if elapsed > _REANCHOR_EVERY_NS:
            self._anchor()
            elapsed = time.perf_counter_ns() - self._perf_anchor
        return self._wall_anchor + elapsed


def _system_clock_is_coarse() -> bool:
    return time.get_clock_info("time").resolution > 1e-6


wall_clock_ns = AnchoredClock().now_ns if _system_clock_is_coarse() else time.time_ns


@lru_cache(maxsize=4096)
def _epoch_seconds(base: str) -> int:
    # Cached: consecutive messages almost always share the same whole second.
    return calendar.timegm(datetime.fromisoformat(base).timetuple())


def rfc3339_to_ns(ts: str) -> int:
    """Parse an RFC3339 UTC timestamp like '2026-10-12T17:22:09.918524312Z' to epoch ns.

    datetime.fromisoformat would truncate to microseconds; Coinbase sends nanoseconds.
    """
    if ts.endswith("Z"):
        ts = ts[:-1]
    elif ts.endswith("+00:00"):
        ts = ts[:-6]
    base, _, frac = ts.partition(".")
    nanos = int(frac[:9].ljust(9, "0")) if frac else 0
    return _epoch_seconds(base) * 1_000_000_000 + nanos
