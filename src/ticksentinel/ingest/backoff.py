"""Reconnect delay policy: exponential backoff with full jitter.

Why jitter: if an exchange drops all clients at once (e.g. Binance's 24 h
disconnect, or a deploy), deterministic backoff makes every client retry at the
same instants and hammer the server in waves. Full jitter (uniform in
[0, cap]) spreads retries out. See the AWS Architecture Blog,
"Exponential Backoff And Jitter".
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class Backoff:
    base_s: float = 0.5
    cap_s: float = 30.0
    rng: random.Random = field(default_factory=random.Random)
    attempt: int = 0

    def next_delay(self) -> float:
        ceiling = min(self.cap_s, self.base_s * (2**self.attempt))
        self.attempt += 1
        return self.rng.uniform(0.0, ceiling)

    def reset(self) -> None:
        self.attempt = 0
