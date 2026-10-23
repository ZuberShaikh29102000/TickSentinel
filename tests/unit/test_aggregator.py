"""SecondAggregator edge cases (hand-built inputs: pure logic, no market data)."""

from __future__ import annotations

from ticksentinel.common.models import Exchange, Side, Tick, TickKind, Trade
from ticksentinel.ingest.aggregator import MarketSecond, SecondAggregator

S = 1_000_000_000
KEY = (Exchange.KRAKEN, "BTC-USD")


def make() -> tuple[SecondAggregator, list[MarketSecond]]:
    rows: list[MarketSecond] = []
    return SecondAggregator(rows.append), rows


def trade_tick(recv_ns: int, *qty_px: tuple[float, float]) -> Tick:
    trades = tuple(Trade(str(i), px, q, Side.BUY, recv_ns) for i, (q, px) in enumerate(qty_px))
    return Tick(Exchange.KRAKEN, "BTC-USD", TickKind.TRADE, None, recv_ns, recv_ns, trades=trades)


def test_nothing_emitted_until_the_second_ends() -> None:
    agg, rows = make()
    agg.on_book(KEY, 100 * S + 1, (99.0, 1.0), (101.0, 2.0))
    agg.flush(100 * S + 999_999_999)
    assert rows == []
    agg.flush(101 * S)
    assert [r.second for r in rows] == [100]


def test_row_keeps_end_of_second_book_and_sums_activity() -> None:
    agg, rows = make()
    agg.on_book(KEY, 100 * S + 10, (99.0, 1.0), (101.0, 2.0))
    agg.on_book(KEY, 100 * S + 20, (99.5, 3.0), (100.5, 4.0))
    agg.on_trades(trade_tick(100 * S + 30, (0.5, 100.0), (0.25, 100.5)))
    agg.flush(101 * S)
    (row,) = rows
    assert (row.best_bid, row.best_bid_qty, row.best_ask, row.best_ask_qty) == (
        99.5,
        3.0,
        100.5,
        4.0,
    )
    assert (row.book_updates, row.trade_count, row.trade_volume) == (2, 2, 0.75)
    assert row.last_trade_px == 100.5


def test_silent_feed_still_produces_rows_flagged_by_zero_updates() -> None:
    """A frozen feed must be visible in storage, not just absent from it."""
    agg, rows = make()
    agg.on_book(KEY, 100 * S, (99.0, 1.0), (101.0, 2.0))
    agg.flush(104 * S)  # 101..103 had no messages at all
    assert [r.second for r in rows] == [100, 101, 102, 103]
    assert [r.book_updates for r in rows] == [1, 0, 0, 0]
    assert all(r.best_bid == 99.0 for r in rows)  # last known state carried forward
    assert all(r.trade_count == 0 and r.last_trade_px is None for r in rows[1:])


def test_message_jumping_seconds_closes_the_skipped_ones() -> None:
    agg, rows = make()
    agg.on_book(KEY, 100 * S, (99.0, 1.0), (101.0, 2.0))
    agg.on_book(KEY, 103 * S + 5, (98.0, 1.0), (102.0, 2.0))  # no flush timer in between
    assert [r.second for r in rows] == [100, 101, 102]
    agg.flush(104 * S)
    assert rows[-1].second == 103 and rows[-1].best_bid == 98.0


def test_slightly_late_message_counts_in_current_second() -> None:
    """Clock re-anchoring can step back < 1 ms; that must not reopen an emitted second."""
    agg, rows = make()
    agg.on_book(KEY, 101 * S + 5, (99.0, 1.0), (101.0, 2.0))
    agg.on_book(KEY, 100 * S + 999_999_000, (99.1, 1.0), (101.0, 2.0))
    agg.flush(102 * S)
    assert [(r.second, r.book_updates) for r in rows] == [(101, 2)]


def test_feeds_are_independent() -> None:
    agg, rows = make()
    other = (Exchange.BINANCE, "BTC-USDT")
    agg.on_book(KEY, 100 * S, (99.0, 1.0), (101.0, 2.0))
    agg.on_book(other, 100 * S, (199.0, 1.0), (201.0, 2.0))
    agg.flush(101 * S)
    assert {(r.exchange, r.best_bid) for r in rows} == {
        (Exchange.KRAKEN, 99.0),
        (Exchange.BINANCE, 199.0),
    }


def test_trades_before_any_book_data() -> None:
    agg, rows = make()
    agg.on_trades(trade_tick(100 * S, (1.0, 100.0)))
    agg.flush(101 * S)
    (row,) = rows
    assert row.best_bid is None and row.best_ask is None
    assert (row.book_updates, row.trade_count) == (0, 1)
