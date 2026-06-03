-- ============================================================================
-- ClickHouse schema for raw Binance trades.
--
-- This file is mounted into /docker-entrypoint-initdb.d and runs ONCE, the
-- first time the container boots on an empty data directory. (Wipe with
-- `make clean` / `docker compose down -v` to force it to run again.)
--
-- Everything uses IF NOT EXISTS so re-running is harmless.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades
(
    -- symbol: only a handful of distinct values (BTCUSDT, ETHUSDT, ...).
    -- LowCardinality dictionary-encodes them: stored as small integer ids with
    -- a string lookup -> less disk, faster GROUP BY / WHERE on symbol.
    symbol       LowCardinality(String),

    -- price/quantity: Float64 for Phase-1 simplicity. Binance sends these as
    -- strings ("64123.45000000"); the consumer parses them to float.
    -- NOTE: floats can't represent every decimal exactly. Fine for charting a
    -- live price. For exact money math later, switch to Decimal64(8).
    price        Float64,
    quantity     Float64,

    -- trade_time: when the trade happened, per Binance (their field "T").
    -- Binance sends epoch MILLISECONDS, so we use DateTime64(3) = ms precision.
    -- Plain DateTime (second precision) would throw away sub-second ordering.
    trade_time   DateTime64(3, 'UTC'),

    -- ingested_at: when WE wrote the row. Defaulted by ClickHouse at insert
    -- time, so the consumer never sets it. (trade_time vs ingested_at lets us
    -- measure end-to-end lag in a later phase.)
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- MergeTree: ClickHouse's core engine. Each INSERT writes an immutable "part"
-- (a folder of column files); a background process merges parts over time.
-- This is exactly why the consumer BATCHES inserts — one big part per batch
-- instead of thousands of tiny parts (which would trigger merge storms and
-- "too many parts" errors).
ENGINE = MergeTree

-- PARTITION BY day: groups parts by calendar day. Makes dropping/expiring old
-- data trivial later (`ALTER TABLE ... DROP PARTITION`, or a TTL) and keeps
-- merges scoped within a day. Don't over-partition (e.g. by minute) — too many
-- partitions hurts.
PARTITION BY toYYYYMMDD(trade_time)

-- ORDER BY defines the sparse primary index AND the on-disk sort order.
-- (symbol, trade_time) matches our main query shape — "price of symbol X over
-- a time range" — so ClickHouse can skip straight to the relevant granules.
ORDER BY (symbol, trade_time);
