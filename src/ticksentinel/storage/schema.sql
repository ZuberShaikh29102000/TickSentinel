-- TickSentinel schema. Plain PostgreSQL 17 (no TimescaleDB): identical on the
-- local container and on Supabase, where TimescaleDB is deprecated for PG 17.
-- Idempotent: the app runs this on every startup, so a fresh database needs no
-- manual setup. Changing an existing table later needs a migration, not an edit.
--
-- Storage budget: Supabase free is 500 MB and goes read-only when exceeded.
-- Retention (storage/maintenance.py) keeps the total around 300 MB.

-- One row per feed per second (~161 bytes measured; ~40 MB/day for 3 venues).
-- Kept 7 days. Used to investigate incidents.
CREATE TABLE IF NOT EXISTS market_1s (
    time           timestamptz      NOT NULL,   -- start of the second
    exchange       text             NOT NULL,
    symbol         text             NOT NULL,
    best_bid       double precision,            -- book state at the end of the second
    best_bid_qty   double precision,
    best_ask       double precision,
    best_ask_qty   double precision,
    book_updates   integer          NOT NULL,   -- 0 = no book data this second (stale/frozen)
    trade_count    integer          NOT NULL,
    trade_volume   double precision NOT NULL,
    last_trade_px  double precision,
    -- time FIRST: every new row then lands at the right-hand end of the index, so
    -- pages fill densely. With (exchange, symbol, time), three feeds insert into
    -- three places at once and pages split half-empty. Measured on 180k rows:
    -- 161 vs 188 bytes/row (39.7 vs 46.4 MB/day). Retention deletes are a single
    -- time-range scan on this index.
    PRIMARY KEY (time, exchange, symbol)
);

-- One row per feed per minute, rolled up from market_1s. Kept 30 days.
-- Used for dashboard charts and trends.
CREATE TABLE IF NOT EXISTS market_1m (
    time            timestamptz      NOT NULL,  -- start of the minute
    exchange        text             NOT NULL,
    symbol          text             NOT NULL,
    mid_open        double precision,
    mid_high        double precision,
    mid_low         double precision,
    mid_close       double precision,
    spread_avg      double precision,
    spread_max      double precision,
    book_updates    integer          NOT NULL,
    trade_count     integer          NOT NULL,
    trade_volume    double precision NOT NULL,
    seconds_present integer          NOT NULL,  -- < 60 means the feed had gaps that minute
    PRIMARY KEY (time, exchange, symbol)
);

-- Connection lifecycle and integrity events from the feed handlers. Kept forever:
-- rare, tiny, and the evidence trail the Phase 4 agents investigate.
CREATE TABLE IF NOT EXISTS feed_events (
    time        timestamptz NOT NULL,
    exchange    text        NOT NULL,
    event_type  text        NOT NULL,
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS feed_events_exchange_time ON feed_events (exchange, time DESC);

-- Events about the system itself rather than a feed (storage guard, maintenance).
CREATE TABLE IF NOT EXISTS system_events (
    time        timestamptz NOT NULL,
    event_type  text        NOT NULL,
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb
);
