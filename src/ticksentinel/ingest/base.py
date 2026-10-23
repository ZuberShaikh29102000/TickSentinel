"""Base class for exchange feed handlers: connection lifecycle and reconnect loop.

Subclasses implement three things: `url`, `subscribe()`, and `on_message()`.
`on_message` is a plain (non-async) method that parses one frame and calls
`emit_tick` / `emit_event`. Keeping parsing synchronous and free of IO means
tests can replay recorded frames straight into it, with no network involved.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Any, ClassVar

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from ticksentinel.common.clock import wall_clock_ns
from ticksentinel.common.models import Exchange, FeedEvent, FeedEventType, Tick
from ticksentinel.ingest.backoff import Backoff

log = logging.getLogger(__name__)

TickHandler = Callable[[Tick], None]
EventHandler = Callable[[FeedEvent], None]
RawHandler = Callable[[str, str, int], None]  # (source: "ws" | "rest", raw text, recv_ns)

# Coinbase's level2 snapshot for BTC-USD is several MB in a single frame;
# websockets' default 1 MiB limit would reject it.
MAX_FRAME_BYTES = 32 * 1024 * 1024


# All three venues send something at least every second (Binance depth every
# 100 ms, Coinbase and Kraken heartbeats every 1 s), so 15 s of silence on an
# open connection is unambiguous: the feed is dead even if TCP looks alive.
DEFAULT_IDLE_TIMEOUT_S = 15.0


class ResyncRequired(Exception):
    """Our view of the book can no longer be trusted; reconnect and start from a fresh snapshot."""


class FeedSilent(Exception):
    """The connection is open but no data has arrived within the idle timeout."""


class FeedClient(ABC):
    exchange: ClassVar[Exchange]

    def __init__(
        self,
        on_tick: TickHandler,
        on_event: EventHandler,
        on_raw: RawHandler | None = None,
        *,
        backoff: Backoff | None = None,
        stable_after_s: float = 60.0,
        idle_timeout_s: float = DEFAULT_IDLE_TIMEOUT_S,
        clock: Callable[[], int] = wall_clock_ns,
    ) -> None:
        self._on_tick = on_tick
        self._on_event = on_event
        self._on_raw = on_raw
        self._backoff = backoff or Backoff()
        self._stable_after_s = stable_after_s
        self._idle_timeout_s = idle_timeout_s
        self._clock = clock
        self.messages_received = 0
        self.sessions_started = 0
        # Read by the /health endpoint.
        self.connected = False
        self.last_message_monotonic: float | None = None

    # ---- subclass interface -------------------------------------------------

    @property
    @abstractmethod
    def url(self) -> str: ...

    async def subscribe(self, ws: ClientConnection) -> None:  # noqa: B027  (optional hook)
        """Send subscription requests. Default: none (subscription encoded in the URL)."""

    @abstractmethod
    def reset_state(self) -> None:
        """Forget all per-connection state (books, sequence numbers). Called before
        every connection: after a reconnect, nothing from the old session is trusted."""

    @abstractmethod
    def on_message(self, raw: str, recv_ns: int) -> None:
        """Parse one frame and emit Ticks/FeedEvents. May raise ResyncRequired."""

    async def on_session_end(self) -> None:  # noqa: B027  (optional hook)
        """Clean up per-session background tasks."""

    # ---- helpers for subclasses ---------------------------------------------

    def emit_tick(self, tick: Tick) -> None:
        self._on_tick(tick)

    def emit_event(
        self, event_type: FeedEventType, detail: Mapping[str, Any] | None = None
    ) -> None:
        self._on_event(FeedEvent(self.exchange, event_type, self._clock(), dict(detail or {})))

    def record_raw(self, source: str, raw: str, recv_ns: int) -> None:
        if self._on_raw is not None:
            self._on_raw(source, raw, recv_ns)

    # ---- lifecycle ----------------------------------------------------------

    async def run(self) -> None:
        """Run forever: connect, stream, and on any failure reconnect with backoff."""
        while True:
            started = time.monotonic()
            reason = await self._run_one_session()
            # Only reset backoff after a session that stayed up for a while; a
            # connect-then-immediately-fail loop must keep backing off.
            if time.monotonic() - started >= self._stable_after_s:
                self._backoff.reset()
            delay = self._backoff.next_delay()
            self.emit_event(
                FeedEventType.DISCONNECTED, {"reason": reason, "reconnect_in_s": round(delay, 3)}
            )
            log.warning("%s disconnected (%s); reconnecting in %.2fs", self.exchange, reason, delay)
            await asyncio.sleep(delay)

    async def _run_one_session(self) -> str:
        """Returns a human-readable reason the session ended. CancelledError propagates."""
        try:
            await self._session()
        except FeedSilent as exc:
            self.emit_event(FeedEventType.WATCHDOG_TIMEOUT, {"idle_s": self._idle_timeout_s})
            return f"watchdog: {exc}"
        except ResyncRequired as exc:
            return f"resync required: {exc}"
        except (OSError, ConnectionClosed, InvalidHandshake, TimeoutError) as exc:
            return f"network: {type(exc).__name__}: {exc}"
        except Exception as exc:
            # Deliberately broad: a parsing bug must not kill the handler forever.
            # It is logged as ERROR so it's visible as possible INTERNAL_BUG evidence.
            log.exception("%s handler error", self.exchange)
            self.emit_event(FeedEventType.ERROR, {"exception": type(exc).__name__, "msg": str(exc)})
            return f"handler error: {type(exc).__name__}: {exc}"
        finally:
            self.connected = False
            await self.on_session_end()
        return "closed by server"

    async def _session(self) -> None:
        self.reset_state()
        self.sessions_started += 1
        async with connect(self.url, max_size=MAX_FRAME_BYTES, open_timeout=10) as ws:
            self.connected = True
            self.emit_event(FeedEventType.CONNECTED, {"url": self.url})
            log.info("%s connected", self.exchange)
            await self.subscribe(ws)
            await self._stream(ws)

    async def _stream(self, ws: ClientConnection) -> None:
        # Websocket pings only prove the TCP connection is alive. A subscription can
        # still silently stop delivering data, which no exception would ever reveal,
        # so the idle deadline is pushed forward on every message instead.
        loop = asyncio.get_running_loop()
        idle = asyncio.timeout_at(loop.time() + self._idle_timeout_s)
        try:
            async with idle:
                async for frame in ws:
                    recv_ns = self._clock()  # first thing: parse time counts toward latency
                    idle.reschedule(loop.time() + self._idle_timeout_s)
                    raw = frame if isinstance(frame, str) else frame.decode()
                    self.messages_received += 1
                    self.last_message_monotonic = time.monotonic()
                    self.record_raw("ws", raw, recv_ns)
                    self.on_message(raw, recv_ns)
        except TimeoutError:
            # The timeout context raises TimeoutError as it exits, so it is caught out
            # here. Only our idle deadline becomes FeedSilent; any other TimeoutError
            # (e.g. from inside a handler) keeps its own meaning.
            if idle.expired():
                raise FeedSilent(f"no data for {self._idle_timeout_s:.0f}s") from None
            raise
