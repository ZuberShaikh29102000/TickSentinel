from __future__ import annotations

import orjson
import pytest

from tests.conftest import Collector, load, replay
from ticksentinel.common.clock import rfc3339_to_ns
from ticksentinel.common.models import FeedEventType, Side, TickKind
from ticksentinel.ingest.base import ResyncRequired
from ticksentinel.ingest.coinbase import CoinbaseClient


def make_client(collector: Collector) -> CoinbaseClient:
    client = CoinbaseClient(collector.on_tick, collector.on_event)
    client.reset_state()
    return client


def test_replay_has_no_gaps_and_starts_with_snapshot(collector: Collector) -> None:
    messages = load("coinbase")
    client = make_client(collector)
    replay(client, messages)

    assert FeedEventType.SEQUENCE_GAP not in collector.event_types()
    assert collector.event_types().count(FeedEventType.SUBSCRIBED) == 3
    book_ticks = [t for t in collector.ticks if t.kind is not TickKind.TRADE]
    assert book_ticks[0].kind is TickKind.BOOK_SNAPSHOT
    assert len(book_ticks[0].bids) > 100 and len(book_ticks[0].asks) > 100
    assert client.last_seq == orjson.loads(messages[-1].raw)["sequence_num"]


def test_dropped_message_raises_resync(collector: Collector) -> None:
    messages = load("coinbase")
    client = make_client(collector)
    drop = 20
    with pytest.raises(ResyncRequired, match="sequence gap"):
        replay(client, messages[:drop] + messages[drop + 1 :])
    gap = next(e for e in collector.events if e.event_type is FeedEventType.SEQUENCE_GAP)
    assert gap.detail["missed"] == 1


def test_historical_trade_snapshot_is_skipped(collector: Collector) -> None:
    messages = load("coinbase")
    snapshot_ids = {
        t["trade_id"]
        for m in messages
        for ev in orjson.loads(m.raw).get("events", [])
        if orjson.loads(m.raw)["channel"] == "market_trades" and ev["type"] == "snapshot"
        for t in ev["trades"]
    }
    assert snapshot_ids, "fixture should contain the subscribe-time trade snapshot"
    client = make_client(collector)
    replay(client, messages)
    emitted = {t.trade_id for tick in collector.of_kind(TickKind.TRADE) for t in tick.trades}
    assert emitted.isdisjoint(snapshot_ids)


def test_trade_normalization_matches_raw(collector: Collector) -> None:
    messages = load("coinbase")
    client = make_client(collector)
    replay(client, messages)
    tick = collector.of_kind(TickKind.TRADE)[0]
    raw_msg = next(
        orjson.loads(m.raw) for m in messages if orjson.loads(m.raw).get("sequence_num") == tick.seq
    )
    raw_trade = raw_msg["events"][0]["trades"][0]
    trade = tick.trades[0]
    assert trade.trade_id == raw_trade["trade_id"]
    assert trade.price == float(raw_trade["price"]) and trade.qty == float(raw_trade["size"])
    # Coinbase reports the maker side; we store the aggressor side.
    assert trade.side is (Side.SELL if raw_trade["side"] == "BUY" else Side.BUY)
    assert trade.exch_ts_ns == rfc3339_to_ns(raw_trade["time"])


def test_book_update_uses_engine_event_time(collector: Collector) -> None:
    messages = load("coinbase")
    client = make_client(collector)
    replay(client, messages)
    tick = collector.of_kind(TickKind.BOOK_UPDATE)[0]
    raw_msg = next(
        orjson.loads(m.raw) for m in messages if orjson.loads(m.raw).get("sequence_num") == tick.seq
    )
    assert tick.exch_ts_ns == rfc3339_to_ns(raw_msg["events"][0]["updates"][-1]["event_time"])
    sides = {u["side"] for u in raw_msg["events"][0]["updates"]}
    assert (len(tick.bids) > 0) == ("bid" in sides)
    assert (len(tick.asks) > 0) == ("offer" in sides)
