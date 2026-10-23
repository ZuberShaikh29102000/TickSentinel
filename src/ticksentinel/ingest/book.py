"""Local L2 order book.

Each side is a SortedDict (price -> qty): O(log n) insert/delete and O(1) access
to the best level. A plain dict would need a full sort on every update to find
the top of book, and Coinbase's snapshot alone is tens of thousands of levels.

Generic over the number type: the pipeline uses float, while the Kraken handler
keeps a Decimal copy because its checksum needs the exact decimal text.
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Generic, TypeVar

from sortedcontainers import SortedDict

N = TypeVar("N", float, Decimal)


class OrderBook(Generic[N]):
    def __init__(self) -> None:
        self.bids: SortedDict = SortedDict()  # ascending; best bid is the LAST item
        self.asks: SortedDict = SortedDict()  # ascending; best ask is the FIRST item

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def update(self, bids: Iterable[tuple[N, N]], asks: Iterable[tuple[N, N]]) -> None:
        """Apply absolute quantities per level (all three venues send absolute size,
        not deltas). Quantity zero means the level is gone."""
        _apply_side(self.bids, bids)
        _apply_side(self.asks, asks)

    def best_bid(self) -> tuple[N, N] | None:
        return self.bids.peekitem(-1) if self.bids else None

    def best_ask(self) -> tuple[N, N] | None:
        return self.asks.peekitem(0) if self.asks else None

    def top_bids(self, n: int) -> list[tuple[N, N]]:
        """Best n bids, highest price first."""
        items = self.bids.items()
        return [items[i] for i in range(len(items) - 1, max(len(items) - n, 0) - 1, -1)]

    def top_asks(self, n: int) -> list[tuple[N, N]]:
        """Best n asks, lowest price first."""
        items = self.asks.items()
        return [items[i] for i in range(min(n, len(items)))]

    def truncate(self, depth: int) -> tuple[list[N], list[N]]:
        """Keep only the best `depth` levels per side. Needed for depth-limited
        feeds (Kraken), which never send qty=0 for levels that scroll out of range.
        Returns the evicted (bid prices, ask prices) so callers can propagate them."""
        evicted_bids = [self.bids.popitem(0)[0] for _ in range(max(len(self.bids) - depth, 0))]
        evicted_asks = [self.asks.popitem(-1)[0] for _ in range(max(len(self.asks) - depth, 0))]
        return evicted_bids, evicted_asks

    def __len__(self) -> int:
        return len(self.bids) + len(self.asks)


def _apply_side(side: SortedDict, levels: Iterable[tuple[N, N]]) -> None:
    for price, qty in levels:
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty
