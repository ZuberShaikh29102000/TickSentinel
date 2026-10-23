"""Normalized market data model.

Every exchange speaks a different dialect; everything downstream of the feed
handlers (detectors, storage, agents) only ever sees these types.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Exchange(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"


class TickKind(StrEnum):
    BOOK_SNAPSHOT = "book_snapshot"  # full replacement of the book
    BOOK_UPDATE = "book_update"  # changed levels only; qty == 0 removes a level
    TRADE = "trade"


class Side(StrEnum):
    """Aggressor (taker) side of a trade. Exchanges disagree on which side they
    report, so each handler converts to this one convention."""

    BUY = "buy"
    SELL = "sell"


class FeedEventType(StrEnum):
    CONNECTED = "connected"
    SUBSCRIBED = "subscribed"
    DISCONNECTED = "disconnected"
    RESYNC = "resync"  # book discarded and rebuilt
    SEQUENCE_GAP = "sequence_gap"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    SERVER_SHUTDOWN = "server_shutdown"
    WATCHDOG_TIMEOUT = "watchdog_timeout"  # connection open but no data: forced reconnect
    ERROR = "error"  # unexpected handler exception (possible internal bug)


@dataclass(frozen=True, slots=True)
class Level:
    # float64, not Decimal: BTC prices have <= 8 significant decimals, well inside
    # float64's ~15-16 digits, and floats are ~50x cheaper to compare and map
    # directly onto numpy/C++ later. Exact decimals are used only where an exchange
    # protocol demands them (Kraken's checksum).
    price: float
    qty: float


@dataclass(frozen=True, slots=True)
class Trade:
    trade_id: str
    price: float
    qty: float
    side: Side
    exch_ts_ns: int


@dataclass(frozen=True, slots=True)
class Tick:
    exchange: Exchange
    symbol: str  # canonical "BASE-QUOTE", e.g. "BTC-USD", "BTC-USDT"
    kind: TickKind
    seq: int | None  # exchange sequence number; None when the venue provides none
    exch_ts_ns: int  # exchange's own timestamp, converted to epoch ns
    recv_ts_ns: int  # our receive time, epoch ns, taken before parsing
    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()
    trades: tuple[Trade, ...] = ()


@dataclass(frozen=True, slots=True)
class FeedEvent:
    """Something that happened to a feed handler rather than to the market."""

    exchange: Exchange
    event_type: FeedEventType
    ts_ns: int
    detail: Mapping[str, Any] = field(default_factory=dict)
