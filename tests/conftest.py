"""Shared test helpers.

Fixtures in tests/fixtures/*.tsv.gz are REAL exchange messages recorded by
scripts/record_samples.sh. Tests that need a fault (gap, corruption) derive it
by editing a copy of real data in the test itself, so the edit is visible.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from ticksentinel.common.models import FeedEvent, FeedEventType, Tick, TickKind
from ticksentinel.ingest.binance import BinanceClient
from ticksentinel.ingest.recorder import RecordedMessage, read_recording

FIXTURES = Path(__file__).parent / "fixtures"


@dataclass
class Collector:
    ticks: list[Tick] = field(default_factory=list)
    events: list[FeedEvent] = field(default_factory=list)

    def on_tick(self, tick: Tick) -> None:
        self.ticks.append(tick)

    def on_event(self, event: FeedEvent) -> None:
        self.events.append(event)

    def event_types(self) -> list[FeedEventType]:
        return [e.event_type for e in self.events]

    def of_kind(self, kind: TickKind) -> list[Tick]:
        return [t for t in self.ticks if t.kind is kind]


class ReplayBinanceClient(BinanceClient):
    """Binance client with the REST fetch stubbed out: snapshots come from the recording."""

    snapshot_requests = 0

    def _request_snapshot(self) -> None:
        if self._snapshot_task is None:
            self.snapshot_requests += 1
            self._snapshot_task = "requested"  # type: ignore[assignment]  # marker only

    def reset_snapshot_request(self) -> None:
        self._snapshot_task = None


def replay(client: object, messages: Iterable[RecordedMessage]) -> None:
    for m in messages:
        if m.source == "rest":
            client.on_rest_snapshot(m.raw, m.recv_ns)  # type: ignore[attr-defined]
        else:
            client.on_message(m.raw, m.recv_ns)  # type: ignore[attr-defined]


def load(exchange: str) -> list[RecordedMessage]:
    path = FIXTURES / f"{exchange}.tsv.gz"
    if not path.exists():
        pytest.skip(f"{path} missing: run scripts/record_samples.sh")
    return list(read_recording(path))


@pytest.fixture
def collector() -> Collector:
    return Collector()
