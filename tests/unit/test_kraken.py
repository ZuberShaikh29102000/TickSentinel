from __future__ import annotations

import json
import re
from decimal import Decimal

import pytest

from tests.conftest import Collector, load, replay
from ticksentinel.common.models import FeedEventType, Side, TickKind
from ticksentinel.ingest.base import ResyncRequired
from ticksentinel.ingest.kraken import KrakenClient, _checksum_token, kraken_checksum


def make_client(collector: Collector) -> KrakenClient:
    client = KrakenClient(collector.on_tick, collector.on_event)
    client.reset_state()
    return client


def test_checksum_matches_kraken_documentation_example() -> None:
    # Worked example from docs.kraken.com/exchange/guides/websockets/book-checksum-v2
    bids = [
        ("45283.5", "0.10000000"),
        ("45283.4", "1.54582015"),
        ("45282.1", "0.10000000"),
        ("45281.0", "0.10000000"),
        ("45280.3", "1.54592586"),
        ("45279.0", "0.07990000"),
        ("45277.6", "0.03310103"),
        ("45277.5", "0.30000000"),
        ("45277.3", "1.54602737"),
        ("45276.6", "0.15445238"),
    ]
    asks = [
        ("45285.2", "0.00100000"),
        ("45286.4", "1.54571953"),
        ("45286.6", "1.54571109"),
        ("45289.6", "1.54560911"),
        ("45290.2", "0.15890660"),
        ("45291.8", "1.54553491"),
        ("45294.7", "0.04454749"),
        ("45296.1", "0.35380000"),
        ("45297.5", "0.09945542"),
        ("45299.5", "0.18772827"),
    ]
    as_dec = lambda levels: [(Decimal(p), Decimal(q)) for p, q in levels]  # noqa: E731
    assert kraken_checksum(as_dec(asks), as_dec(bids)) == 3310070434


def test_checksum_token_never_uses_scientific_notation() -> None:
    assert str(Decimal("0.00000051")) == "5.1E-7"  # the trap
    assert _checksum_token(Decimal("0.00000051")) == "51"
    assert _checksum_token(Decimal("0.00100000")) == "100000"


def test_replay_verifies_every_book_checksum(collector: Collector) -> None:
    messages = load("kraken")
    client = make_client(collector)
    replay(client, messages)

    book_msgs = [m for m in messages if m.raw.startswith('{"channel":"book"')]
    assert client.checksums_verified == len(book_msgs)  # every single one matched Kraken
    assert FeedEventType.CHECKSUM_MISMATCH not in collector.event_types()
    assert len(client.book.bids) <= client.depth and len(client.book.asks) <= client.depth


def test_corrupted_quantity_is_detected(collector: Collector) -> None:
    messages = load("kraken")
    book_updates = [
        i
        for i, m in enumerate(messages)
        if m.raw.startswith('{"channel":"book"')
        and '"type":"update"' in m.raw
        and re.search(r'"qty":[1-9]', m.raw)  # a non-zero qty, so the level stays in the book
    ]
    idx = book_updates[10]
    # Corrupt one quantity by a single satoshi, keeping Kraken's 8-decimal formatting.
    match = re.search(r'"qty":([1-9][0-9]*\.[0-9]{8})', messages[idx].raw)
    assert match is not None
    bumped = format(Decimal(match.group(1)) + Decimal("0.00000001"), "f")
    corrupted = messages[idx].raw.replace(match.group(0), f'"qty":{bumped}', 1)

    client = make_client(collector)
    replay(client, messages[:idx])
    with pytest.raises(ResyncRequired, match="checksum mismatch"):
        client.on_message(corrupted, messages[idx].recv_ns)
    assert collector.event_types()[-1] is FeedEventType.CHECKSUM_MISMATCH


def test_trade_normalization_matches_raw(collector: Collector) -> None:
    messages = load("kraken")
    raw = next(m for m in messages if m.raw.startswith('{"channel":"trade"'))
    client = make_client(collector)
    client.on_message(raw.raw, raw.recv_ns)

    data = json.loads(raw.raw, parse_float=Decimal)["data"]
    (tick,) = collector.of_kind(TickKind.TRADE)
    assert [t.trade_id for t in tick.trades] == [str(d["trade_id"]) for d in data]
    assert [t.side for t in tick.trades] == [Side(d["side"]) for d in data]
    assert [t.price for t in tick.trades] == [float(d["price"]) for d in data]
    assert tick.symbol == "BTC-USD" and tick.seq is None


def test_lost_update_self_heals_when_same_level_is_overwritten(collector: Collector) -> None:
    """Why a lost Kraken update is NOT always detectable, and why that is fine.

    Real data: message 9 sets ask 82358.4 -> 1.28787898; message 10 sets the same
    level to 1.25166529. Quantities are absolute, so losing message 9 leaves the book
    identical to Kraken's from message 10 on, and every later checksum matches.
    (Measured on real recordings: ~16-20% of single lost updates heal like this.)
    """
    messages = load("kraken")
    drop = 9
    assert '"price":82358.4' in messages[drop].raw and '"price":82358.4' in messages[drop + 1].raw

    true_book, lossy = make_client(Collector()), make_client(collector)
    for i, m in enumerate(messages[:200]):
        true_book.on_message(m.raw, m.recv_ns)
        if i != drop:
            lossy.on_message(m.raw, m.recv_ns)  # would raise ResyncRequired on any mismatch

    assert FeedEventType.CHECKSUM_MISMATCH not in collector.event_types()
    assert lossy.book.top_bids(10) == true_book.book.top_bids(10)
    assert lossy.book.top_asks(10) == true_book.book.top_asks(10)
