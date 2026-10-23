"""Coinbase Advanced Trade feed handler (level2 + market_trades + heartbeats).

Docs: https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels

Integrity model: every message on a connection carries `sequence_num`, a
per-*connection* counter shared by all channels (we observed it counting
`subscriptions` and `heartbeats` messages too). Any jump means a message was
lost. Unlike Binance there is no REST snapshot to stitch onto the stream, so the
only way to rebuild the book is to resubscribe, i.e. reconnect.
"""

from __future__ import annotations

import logging
from typing import Any

import orjson
from websockets.asyncio.client import ClientConnection

from ticksentinel.common.clock import rfc3339_to_ns
from ticksentinel.common.models import (
    Exchange,
    FeedEventType,
    Level,
    Side,
    Tick,
    TickKind,
    Trade,
)
from ticksentinel.ingest.base import FeedClient, ResyncRequired

log = logging.getLogger(__name__)

WS_URL = "wss://advanced-trade-ws.coinbase.com"
# heartbeats: without it, Coinbase closes channels after 60-90 s of no updates.
CHANNELS = ("level2", "market_trades", "heartbeats")


class CoinbaseClient(FeedClient):
    exchange = Exchange.COINBASE

    def __init__(self, *args: Any, product_id: str = "BTC-USD", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.product_id = product_id
        self.last_seq: int | None = None

    @property
    def url(self) -> str:
        return WS_URL

    async def subscribe(self, ws: ClientConnection) -> None:
        for channel in CHANNELS:
            msg: dict[str, Any] = {"type": "subscribe", "channel": channel}
            if channel != "heartbeats":
                msg["product_ids"] = [self.product_id]
            await ws.send(orjson.dumps(msg).decode())

    def reset_state(self) -> None:
        self.last_seq = None

    def on_message(self, raw: str, recv_ns: int) -> None:
        msg = orjson.loads(raw)
        if msg.get("type") == "error":
            self.emit_event(FeedEventType.ERROR, {"message": msg.get("message")})
            raise ResyncRequired(f"exchange error: {msg.get('message')}")

        self._check_sequence(msg["sequence_num"])
        channel = msg.get("channel")
        if channel == "l2_data":
            self._on_l2(msg, recv_ns)
        elif channel == "market_trades":
            self._on_trades(msg, recv_ns)
        elif channel == "subscriptions":
            self.emit_event(FeedEventType.SUBSCRIBED, {"subscriptions": msg["events"][0]})
        # heartbeats: nothing to do beyond the sequence check above.

    def _check_sequence(self, seq: int) -> None:
        if self.last_seq is not None and seq != self.last_seq + 1:
            self.emit_event(
                FeedEventType.SEQUENCE_GAP,
                {"expected": self.last_seq + 1, "got": seq, "missed": seq - self.last_seq - 1},
            )
            raise ResyncRequired(f"sequence gap: expected {self.last_seq + 1}, got {seq}")
        self.last_seq = seq

    def _on_l2(self, msg: dict[str, Any], recv_ns: int) -> None:
        for event in msg["events"]:
            updates = event["updates"]
            bids = tuple(
                Level(float(u["price_level"]), float(u["new_quantity"]))
                for u in updates
                if u["side"] == "bid"
            )
            asks = tuple(
                Level(float(u["price_level"]), float(u["new_quantity"]))
                for u in updates
                if u["side"] == "offer"
            )
            # event_time is the matching engine's time for the change; the envelope
            # timestamp is only when Coinbase's gateway sent the message. Updates in
            # one event share an event_time, so the last one is representative.
            exch_ts = rfc3339_to_ns(updates[-1]["event_time"] if updates else msg["timestamp"])
            self.emit_tick(
                Tick(
                    exchange=self.exchange,
                    symbol=event["product_id"],
                    kind=TickKind.BOOK_SNAPSHOT
                    if event["type"] == "snapshot"
                    else TickKind.BOOK_UPDATE,
                    seq=msg["sequence_num"],
                    exch_ts_ns=exch_ts,
                    recv_ts_ns=recv_ns,
                    bids=bids,
                    asks=asks,
                )
            )

    def _on_trades(self, msg: dict[str, Any], recv_ns: int) -> None:
        for event in msg["events"]:
            # The subscribe-time "snapshot" is a batch of *historical* trades. Emitting
            # them would look like stale/duplicate data to the detectors, so skip them.
            if event["type"] != "update":
                continue
            trades = tuple(
                Trade(
                    trade_id=t["trade_id"],
                    price=float(t["price"]),
                    qty=float(t["size"]),
                    # Coinbase documents `side` as the MAKER's side; the aggressor is
                    # the opposite.
                    side=Side.SELL if t["side"] == "BUY" else Side.BUY,
                    exch_ts_ns=rfc3339_to_ns(t["time"]),
                )
                for t in event["trades"]
            )
            if not trades:
                continue
            self.emit_tick(
                Tick(
                    exchange=self.exchange,
                    symbol=event["trades"][0]["product_id"],
                    kind=TickKind.TRADE,
                    seq=msg["sequence_num"],
                    exch_ts_ns=max(t.exch_ts_ns for t in trades),
                    recv_ts_ns=recv_ns,
                    trades=trades,
                )
            )
