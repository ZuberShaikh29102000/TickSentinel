from __future__ import annotations

import orjson
import pytest

from tests.conftest import Collector, ReplayBinanceClient, load, replay
from ticksentinel.common.models import FeedEventType, Side, TickKind
from ticksentinel.ingest.base import ResyncRequired
from ticksentinel.ingest.binance import DepthSequencer, Verdict


def make_client(collector: Collector) -> ReplayBinanceClient:
    client = ReplayBinanceClient(collector.on_tick, collector.on_event)
    client.reset_state()
    return client


def depth_events(messages: list) -> list[dict]:
    return [
        orjson.loads(m.raw)["data"]
        for m in messages
        if m.source == "ws" and '"depthUpdate"' in m.raw
    ]


# ---- DepthSequencer: pure logic ------------------------------------------------


def test_sequencer_rejects_snapshot_older_than_stream() -> None:
    seq = DepthSequencer()
    seq.buffer.append(({"U": 100, "u": 110}, 0))
    assert seq.on_snapshot(99) is None  # lastUpdateId < first U: refetch
    assert not seq.synced


def test_sequencer_classifies_events_against_book_id() -> None:
    seq = DepthSequencer()
    seq.buffer.append(({"U": 100, "u": 110}, 0))
    assert seq.on_snapshot(105) is not None
    assert seq.check({"U": 100, "u": 105}) is Verdict.STALE  # fully covered by snapshot
    assert seq.check({"U": 100, "u": 110}) is Verdict.APPLY  # straddles the snapshot id
    assert seq.book_update_id == 110
    assert seq.check({"U": 111, "u": 120}) is Verdict.APPLY  # U == previous u + 1
    assert seq.check({"U": 125, "u": 130}) is Verdict.GAP  # 121..124 missing


# ---- full replay of a real recording -----------------------------------------


def test_replay_syncs_once_without_gaps(collector: Collector) -> None:
    messages = load("binance")
    client = make_client(collector)
    replay(client, messages)

    assert client.sequencer.synced
    assert collector.event_types() == []  # no gaps, no resyncs
    snapshots = collector.of_kind(TickKind.BOOK_SNAPSHOT)
    assert len(snapshots) == 1
    assert len(snapshots[0].bids) == 1000 and len(snapshots[0].asks) == 1000
    # Book id must end at the final u of the stream: every event was accounted for.
    assert client.sequencer.book_update_id == depth_events(messages)[-1]["u"]


def test_replay_discards_events_covered_by_snapshot(collector: Collector) -> None:
    messages = load("binance")
    client = make_client(collector)
    replay(client, messages)

    snapshot_id = collector.of_kind(TickKind.BOOK_SNAPSHOT)[0].seq
    expected_updates = [e for e in depth_events(messages) if e["u"] > snapshot_id]
    updates = collector.of_kind(TickKind.BOOK_UPDATE)
    assert [t.seq for t in updates] == [e["u"] for e in expected_updates]


def test_stale_snapshot_is_rejected_and_refetched(collector: Collector) -> None:
    messages = load("binance")
    client = make_client(collector)
    first_ws = next(m for m in messages if '"depthUpdate"' in m.raw)
    client.on_message(first_ws.raw, first_ws.recv_ns)
    rest = next(m for m in messages if m.source == "rest")

    # Edit the real snapshot to predate the first buffered event.
    snap = orjson.loads(rest.raw)
    snap["lastUpdateId"] = orjson.loads(first_ws.raw)["data"]["U"] - 1
    assert client.on_rest_snapshot(orjson.dumps(snap).decode(), rest.recv_ns) is False
    assert collector.of_kind(TickKind.BOOK_SNAPSHOT) == []


def test_dropped_depth_message_triggers_gap_and_resync(collector: Collector) -> None:
    messages = load("binance")
    rest_idx = next(i for i, m in enumerate(messages) if m.source == "rest")
    depth_after = [i for i, m in enumerate(messages) if i > rest_idx and '"depthUpdate"' in m.raw]
    dropped = depth_after[5]  # simulate one lost packet, well after sync
    client = make_client(collector)
    replay(client, messages[:dropped] + messages[dropped + 1 :])

    types = collector.event_types()
    assert FeedEventType.SEQUENCE_GAP in types
    assert FeedEventType.RESYNC in types
    gap = next(e for e in collector.events if e.event_type is FeedEventType.SEQUENCE_GAP)
    lost = orjson.loads(messages[dropped].raw)["data"]
    assert gap.detail["missed"] == lost["u"] - lost["U"] + 1
    # Book is discarded and a new snapshot requested (the recording has no second one).
    assert not client.sequencer.synced
    assert client.snapshot_requests == 2


def test_trade_normalization_matches_raw(collector: Collector) -> None:
    messages = load("binance")
    raw = next(m for m in messages if '"e":"trade"' in m.raw)
    client = make_client(collector)
    client.on_message(raw.raw, raw.recv_ns)

    data = orjson.loads(raw.raw)["data"]
    (tick,) = collector.ticks
    (trade,) = tick.trades
    assert tick.kind is TickKind.TRADE and tick.symbol == "BTC-USDT"
    assert trade.trade_id == str(data["t"])
    assert trade.price == float(data["p"]) and trade.qty == float(data["q"])
    assert trade.side is (Side.SELL if data["m"] else Side.BUY)
    assert trade.exch_ts_ns == data["T"] * 1000  # microseconds -> ns
    assert tick.recv_ts_ns == raw.recv_ns


@pytest.mark.parametrize(("maker_is_buyer", "aggressor"), [(True, Side.SELL), (False, Side.BUY)])
def test_trade_side_is_aggressor(
    collector: Collector, maker_is_buyer: bool, aggressor: Side
) -> None:
    client = make_client(collector)
    msg = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1,
            "s": "BTCUSDT",
            "t": 7,
            "p": "1.0",
            "q": "2.0",
            "T": 1,
            "m": maker_is_buyer,
        },
    }
    client.on_message(orjson.dumps(msg).decode(), 0)
    assert collector.ticks[0].trades[0].side is aggressor


def test_server_shutdown_forces_reconnect(collector: Collector) -> None:
    # Shape from Binance docs ("Server Shutdown" section).
    client = make_client(collector)
    msg = {"stream": "!serverShutdown", "data": {"e": "serverShutdown", "E": 1770123456789}}
    with pytest.raises(ResyncRequired):
        client.on_message(orjson.dumps(msg).decode(), 0)
    assert collector.event_types() == [FeedEventType.SERVER_SHUTDOWN]
