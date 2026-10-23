"""Print what is in the DB and how much of the storage budget it uses:
`python -m ticksentinel.storage.check`."""

from __future__ import annotations

import asyncio

import asyncpg

from ticksentinel.common.config import get_settings

QUERIES = {
    "market_1s": """
        SELECT exchange, symbol, count(*) AS rows, min(time) AS first, max(time) AS last,
               sum(book_updates) AS book_updates, sum(trade_count) AS trades,
               count(*) FILTER (WHERE book_updates = 0) AS seconds_without_book_data,
               count(*) FILTER (WHERE best_bid >= best_ask) AS crossed_seconds
        FROM market_1s GROUP BY exchange, symbol ORDER BY exchange""",
    "market_1m": """
        SELECT exchange, symbol, count(*) AS rows, min(time) AS first, max(time) AS last
        FROM market_1m GROUP BY exchange, symbol ORDER BY exchange""",
    "feed_events": """
        SELECT exchange, event_type, count(*) AS rows, max(time) AS last
        FROM feed_events GROUP BY exchange, event_type ORDER BY exchange, event_type""",
    "system_events": """
        SELECT event_type, count(*) AS rows, max(time) AS last
        FROM system_events GROUP BY event_type ORDER BY event_type""",
    "storage": """
        SELECT relname AS table, pg_size_pretty(pg_total_relation_size(oid)) AS size,
               pg_total_relation_size(oid) AS bytes
        FROM pg_class
        WHERE relname IN ('market_1s', 'market_1m', 'feed_events', 'system_events')
        UNION ALL
        SELECT 'DATABASE TOTAL (budget 500 MB, guard 400 MB)',
               pg_size_pretty(pg_database_size(current_database())),
               pg_database_size(current_database())""",
}


def print_table(title: str, rows: list[asyncpg.Record]) -> None:
    print(f"\n== {title}")
    if not rows:
        print("  (empty)")
        return
    cols = list(rows[0].keys())
    cells = [[str(r[c]) for c in cols] for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)]
    print("  " + "  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    for row in cells:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))


async def main() -> None:
    conn = await asyncpg.connect(get_settings().postgres_dsn)
    try:
        for title, sql in QUERIES.items():
            print_table(title, await conn.fetch(sql))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
