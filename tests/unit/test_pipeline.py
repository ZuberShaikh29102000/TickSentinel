"""End-to-end over real recordings: raw messages -> handlers -> pipeline -> books
and per-second storage rows."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from tests.conftest import ReplayBinanceClient, load, replay
from ticksentinel.common.models import Exchange, FeedEvent, Tick, TickKind
from ticksentinel.ingest.aggregator import MarketSecond
from ticksentinel.ingest.base import FeedClient
from ticksentinel.ingest.coinbase import CoinbaseClient
from ticksentinel.ingest.kraken import KrakenClient
from ticksentinel.ingest.pipeline import BookTop, Pipeline


@dataclass
class Replayed:
    name: str
    tops: list[BookTop] = field(default_factory=list)  # book view after every book tick
    ticks: list[Tick] = field(default_factory=list)
    seconds: list[MarketSecond] = field(default_factory=list)
    events: list[FeedEvent] = field(default_factory=list)

    # RowSink protocol
    def add_market_second(self, row: MarketSecond) -> None:
        self.seconds.append(row)

    def add_event(self, event: FeedEvent) -> None:
        self.events.append(event)


CLIENTS: dict[str, type[FeedClient]] = {
    "binance": ReplayBinanceClient,
    "coinbase": CoinbaseClient,
    "kraken": KrakenClient,
}


@pytest.fixture(params=list(CLIENTS))
def replayed(request: pytest.FixtureRequest) -> Replayed:
    out = Replayed(request.param)
    pipeline = Pipeline(out, depth=10)

    def on_tick(tick: Tick) -> None:
        pipeline.on_tick(tick)
        out.ticks.append(tick)
        if tick.kind is not TickKind.TRADE:
            out.tops.append(pipeline.last_top[(tick.exchange, tick.symbol)])

    client = CLIENTS[out.name](on_tick, pipeline.on_event)
    client.reset_state()
    messages = load(out.name)
    replay(client, messages)
    # Close every second up to (and including) the last recorded one.
    pipeline.flush_seconds(messages[-1].recv_ns + 1_000_000_000)
    return out


# ---- the in-memory book (what detection will see) ------------------------------


def test_real_books_are_never_crossed(replayed: Replayed) -> None:
    """A correctly maintained book from a healthy feed always has bid < ask.
    If this fails on real data, our book-building logic is wrong."""
    assert replayed.tops, f"{replayed.name}: no book updates"
    for top in replayed.tops:
        assert top.bids and top.asks, f"{replayed.name}: empty side at seq {top.seq}"
        assert top.bids[0][0] < top.asks[0][0], f"{replayed.name}: crossed at seq {top.seq}"


def test_tops_are_sorted_and_bounded(replayed: Replayed) -> None:
    for top in replayed.tops:
        bid_px = [p for p, _ in top.bids]
        ask_px = [p for p, _ in top.asks]
        assert bid_px == sorted(bid_px, reverse=True) and ask_px == sorted(ask_px)
        assert len(bid_px) <= 10 and len(ask_px) <= 10


def test_first_book_update_is_a_snapshot(replayed: Replayed) -> None:
    assert replayed.tops[0].kind is TickKind.BOOK_SNAPSHOT
    assert all(t.kind is TickKind.BOOK_UPDATE for t in replayed.tops[1:])


def test_receive_timestamps_are_monotonic(replayed: Replayed) -> None:
    recv = [t.recv_ts_ns for t in replayed.tops if t.kind is TickKind.BOOK_UPDATE]
    assert recv == sorted(recv)


# ---- the per-second rows (what storage receives) --------------------------------


def test_one_row_per_second_with_no_gaps(replayed: Replayed) -> None:
    seconds = [r.second for r in replayed.seconds]
    assert seconds, f"{replayed.name}: no per-second rows"
    assert seconds == list(range(seconds[0], seconds[0] + len(seconds)))
    assert {r.exchange for r in replayed.seconds} == {Exchange(replayed.name)}


def test_seconds_account_for_every_tick(replayed: Replayed) -> None:
    """Summarizing must not lose anything: totals match the ticks exactly."""
    book_ticks = [t for t in replayed.ticks if t.kind is not TickKind.TRADE]
    trades = [tr for t in replayed.ticks if t.kind is TickKind.TRADE for tr in t.trades]
    assert sum(r.book_updates for r in replayed.seconds) == len(book_ticks)
    assert sum(r.trade_count for r in replayed.seconds) == len(trades)
    assert sum(r.trade_volume for r in replayed.seconds) == pytest.approx(
        sum(tr.qty for tr in trades)
    )
    assert trades, f"{replayed.name}: no trades in a 45 s BTC recording is implausible"


def test_second_rows_carry_end_of_second_book(replayed: Replayed) -> None:
    """Each row's best bid/ask is the book after the LAST update in that second."""
    last_top_by_second: dict[int, BookTop] = {}
    for top in replayed.tops:
        last_top_by_second[top.recv_ts_ns // 1_000_000_000] = top
    for row in replayed.seconds:
        top = last_top_by_second.get(row.second)
        if top is None:
            assert row.book_updates == 0
            continue
        assert (row.best_bid, row.best_bid_qty) == top.bids[0]
        assert (row.best_ask, row.best_ask_qty) == top.asks[0]


def test_kraken_pipeline_book_matches_checksum_verified_book() -> None:
    """Regression: Kraken never sends deletes for levels that scroll past the subscribed
    depth. The pipeline's book must still end up identical to the handler's
    checksum-verified book after every single message, or stale levels leak into
    the top of book (seen live as a crossed book: bid 82378.4 > ask 82362.8)."""
    pipeline = Pipeline(depth=10)
    client = KrakenClient(pipeline.on_tick, pipeline.on_event)
    client.reset_state()
    checked = 0
    for m in load("kraken"):
        client.on_message(m.raw, m.recv_ns)
        top = pipeline.last_top.get((Exchange.KRAKEN, "BTC-USD"))
        if top is None:
            continue
        verified_bids = [(float(p), float(q)) for p, q in client.book.top_bids(10)]
        verified_asks = [(float(p), float(q)) for p, q in client.book.top_asks(10)]
        assert top.bids == verified_bids and top.asks == verified_asks, f"diverged at msg {checked}"
        checked += 1
    assert checked > 1000
