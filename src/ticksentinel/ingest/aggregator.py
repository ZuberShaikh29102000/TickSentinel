"""Per-second summaries of each feed: what we store, instead of every tick.

Measured on our own data, storing every book update and trade costs ~3 GB/day,
while the live database has a 500 MB budget. Detection still sees every tick in
memory (see pipeline.py); only *storage* is summarized, to one row per feed per
second (~156 bytes, ~40 MB/day for 3 venues).

A row is emitted for EVERY second once a feed has started, even seconds with no
messages. A frozen feed therefore shows up in the data as rows with
book_updates = 0, instead of silently disappearing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ticksentinel.common.models import Exchange, Tick

NS_PER_S = 1_000_000_000

FeedKey = tuple[Exchange, str]


@dataclass(frozen=True, slots=True)
class MarketSecond:
    second: int  # epoch second this row covers: [second, second + 1)
    exchange: Exchange
    symbol: str
    # Book state at the END of the second. Carried forward through seconds with
    # no updates; book_updates == 0 is what marks those seconds as stale.
    best_bid: float | None
    best_bid_qty: float | None
    best_ask: float | None
    best_ask_qty: float | None
    book_updates: int
    trade_count: int
    trade_volume: float
    last_trade_px: float | None


@dataclass(slots=True)
class _FeedState:
    second: int
    best_bid: tuple[float, float] | None = None
    best_ask: tuple[float, float] | None = None
    book_updates: int = 0
    trade_count: int = 0
    trade_volume: float = 0.0
    last_trade_px: float | None = None

    def close(self, key: FeedKey) -> MarketSecond:
        bid, ask = self.best_bid, self.best_ask
        return MarketSecond(
            second=self.second,
            exchange=key[0],
            symbol=key[1],
            best_bid=bid[0] if bid else None,
            best_bid_qty=bid[1] if bid else None,
            best_ask=ask[0] if ask else None,
            best_ask_qty=ask[1] if ask else None,
            book_updates=self.book_updates,
            trade_count=self.trade_count,
            trade_volume=self.trade_volume,
            last_trade_px=self.last_trade_px,
        )

    def advance(self) -> None:
        """Move to the next second: counters reset, book state carries forward."""
        self.second += 1
        self.book_updates = self.trade_count = 0
        self.trade_volume = 0.0
        self.last_trade_px = None


class SecondAggregator:
    def __init__(self, emit: Callable[[MarketSecond], None]) -> None:
        self._emit = emit
        self._feeds: dict[FeedKey, _FeedState] = {}

    def on_book(
        self,
        key: FeedKey,
        recv_ns: int,
        best_bid: tuple[float, float] | None,
        best_ask: tuple[float, float] | None,
    ) -> None:
        state = self._state_at(key, recv_ns // NS_PER_S)
        state.best_bid, state.best_ask = best_bid, best_ask
        state.book_updates += 1

    def on_trades(self, tick: Tick) -> None:
        state = self._state_at((tick.exchange, tick.symbol), tick.recv_ts_ns // NS_PER_S)
        for trade in tick.trades:
            state.trade_count += 1
            state.trade_volume += trade.qty
            state.last_trade_px = trade.price

    def flush(self, now_ns: int) -> None:
        """Emit every second that has fully ended. Called from a 1 s timer, so
        seconds are emitted even for a feed that has gone completely silent."""
        current = now_ns // NS_PER_S
        for key, state in self._feeds.items():
            self._close_until(key, state, current)

    def _state_at(self, key: FeedKey, second: int) -> _FeedState:
        state = self._feeds.get(key)
        if state is None:
            state = self._feeds[key] = _FeedState(second)
        else:
            # A message from a slightly earlier second (clock re-anchoring can step
            # back by < 1 ms) is counted in the current second rather than reopening
            # one already emitted.
            self._close_until(key, state, second)
        return state

    def _close_until(self, key: FeedKey, state: _FeedState, second: int) -> None:
        while state.second < second:
            self._emit(state.close(key))
            state.advance()
