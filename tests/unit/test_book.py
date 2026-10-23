from __future__ import annotations

from ticksentinel.ingest.book import OrderBook


def test_update_insert_replace_remove() -> None:
    book: OrderBook[float] = OrderBook()
    book.update(bids=[(100.0, 1.0), (99.0, 2.0)], asks=[(101.0, 1.5)])
    assert book.best_bid() == (100.0, 1.0)
    assert book.best_ask() == (101.0, 1.5)

    book.update(bids=[(100.0, 3.0)], asks=[])  # absolute size replaces, not adds
    assert book.best_bid() == (100.0, 3.0)

    book.update(bids=[(100.0, 0.0)], asks=[])  # zero removes the level
    assert book.best_bid() == (99.0, 2.0)


def test_removing_unknown_level_is_harmless() -> None:
    book: OrderBook[float] = OrderBook()
    book.update(bids=[(50.0, 0.0)], asks=[])
    assert len(book) == 0


def test_top_levels_are_best_first() -> None:
    book: OrderBook[float] = OrderBook()
    book.update(
        bids=[(p, 1.0) for p in (95.0, 99.0, 97.0)], asks=[(p, 1.0) for p in (103.0, 101.0, 102.0)]
    )
    assert [p for p, _ in book.top_bids(2)] == [99.0, 97.0]
    assert [p for p, _ in book.top_asks(2)] == [101.0, 102.0]
    assert len(book.top_bids(10)) == 3  # asking for more than exists is fine


def test_truncate_drops_worst_levels() -> None:
    book: OrderBook[float] = OrderBook()
    book.update(
        bids=[(float(p), 1.0) for p in range(90, 100)],
        asks=[(float(p), 1.0) for p in range(101, 111)],
    )
    book.truncate(3)
    assert [p for p, _ in book.top_bids(10)] == [99.0, 98.0, 97.0]
    assert [p for p, _ in book.top_asks(10)] == [101.0, 102.0, 103.0]


def test_empty_book() -> None:
    book: OrderBook[float] = OrderBook()
    assert book.best_bid() is None and book.best_ask() is None
    assert book.top_bids(5) == [] and book.top_asks(5) == []


def test_truncate_reports_evicted_prices() -> None:
    book: OrderBook[float] = OrderBook()
    book.update(bids=[(98.0, 1.0), (99.0, 1.0)], asks=[(101.0, 1.0), (102.0, 1.0), (103.0, 1.0)])
    assert book.truncate(1) == ([98.0], [103.0, 102.0])
    assert book.truncate(1) == ([], [])
