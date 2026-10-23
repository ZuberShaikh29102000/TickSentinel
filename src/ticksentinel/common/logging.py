"""Logging: human-readable on the console, JSON lines on disk.

The JSON file is machine-readable on purpose: in Phase 4 the investigation
agents read handler logs as evidence, and parsing free text would be fragile.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

import orjson

# Attributes every LogRecord has; anything else was passed via `extra=` and is
# included in the JSON output as structured context.
_STANDARD_ATTRS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S") + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in _STANDARD_ATTRS})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return orjson.dumps(payload, default=str).decode()


def setup_logging(log_dir: Path | None = None, level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root.addHandler(console)

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "ticksentinel.jsonl", encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)

    logging.Formatter.converter = time.gmtime  # log timestamps in UTC, like exchange timestamps
