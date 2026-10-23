from __future__ import annotations

from pathlib import Path

from ticksentinel.common.models import Exchange
from ticksentinel.ingest.recorder import RawRecorder, read_recording


async def test_round_trip_preserves_exact_text(tmp_path: Path) -> None:
    recorder = RawRecorder(tmp_path, [Exchange.KRAKEN, Exchange.BINANCE])
    append_k = recorder.appender(Exchange.KRAKEN)
    append_b = recorder.appender(Exchange.BINANCE)

    raw = '{"channel":"book","data":[{"price":82515.4,"qty":0.00161338}]}'
    append_k("ws", raw, 111)
    await recorder.flush()  # lines written across two flushes...
    append_k("ws", raw + " ", 222)  # ...and exact bytes preserved (trailing space)
    append_b("rest", '{"lastUpdateId":5}', 333)
    await recorder.close()

    kraken = list(read_recording(recorder.paths[Exchange.KRAKEN]))
    assert [(m.recv_ns, m.source, m.raw) for m in kraken] == [
        (111, "ws", raw),
        (222, "ws", raw + " "),
    ]
    binance = list(read_recording(recorder.paths[Exchange.BINANCE]))
    assert [(m.source, m.raw) for m in binance] == [("rest", '{"lastUpdateId":5}')]
    assert recorder.lines_written == 3
