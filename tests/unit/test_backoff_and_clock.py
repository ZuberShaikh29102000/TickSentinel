from __future__ import annotations

import random
import time

import pytest

from ticksentinel.common.clock import AnchoredClock, rfc3339_to_ns
from ticksentinel.ingest.backoff import Backoff


def test_backoff_ceiling_doubles_then_caps() -> None:
    # rng that always returns the upper bound exposes the ceiling itself
    class MaxRng(random.Random):
        def uniform(self, a: float, b: float) -> float:
            return b

    b = Backoff(base_s=0.5, cap_s=4.0, rng=MaxRng())
    assert [b.next_delay() for _ in range(6)] == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]
    b.reset()
    assert b.next_delay() == 0.5


def test_backoff_is_jittered_within_bounds() -> None:
    b = Backoff(base_s=1.0, cap_s=30.0, rng=random.Random(42))
    delays = [b.next_delay() for _ in range(50)]
    assert all(0.0 <= d <= 30.0 for d in delays)
    assert len(set(delays)) == 50  # jitter: no two retries at the same instant


@pytest.mark.parametrize(
    ("ts", "expected_ns"),
    [
        ("2026-10-12T17:22:09.918524312Z", 1791825729_918524312),  # Coinbase: 9 digits
        ("2026-10-12T17:22:09.617585Z", 1791825729_617585000),  # 6 digits
        ("2026-10-12T17:22:09.5Z", 1791825729_500000000),
        ("2026-10-12T17:22:09Z", 1791825729_000000000),  # no fraction
        ("2026-10-12T17:22:09.123456+00:00", 1791825729_123456000),
    ],
)
def test_rfc3339_to_ns_keeps_full_precision(ts: str, expected_ns: int) -> None:
    assert rfc3339_to_ns(ts) == expected_ns


def test_anchored_clock_is_close_to_wall_time_and_fine_grained() -> None:
    clock = AnchoredClock()
    assert abs(clock.now_ns() - time.time_ns()) < 50_000_000  # within 50 ms of system time
    readings = [clock.now_ns() for _ in range(1000)]
    assert readings == sorted(readings)
    # Sub-microsecond steps: far finer than the ~1-15 ms GetSystemTimeAsFileTime clock.
    assert len(set(readings)) > 900
