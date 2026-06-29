-- ============================================================================
-- ClickHouse schema for raw Binance trades.
--
-- This file is mounted into /docker-entrypoint-initdb.d and runs ONCE, the
-- first time the container boots on an empty data directory. (Wipe with
-- `make clean` / `docker compose down -v` to force it to run again.)
--
-- Phase 3 change: engine MergeTree -> ReplacingMergeTree to make ingestion
-- IDEMPOTENT (effectively exactly-once). See the big note on the engine below.
-- Everything uses IF NOT EXISTS so re-running is harmless.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades
(
    -- symbol: only a handful of distinct values (BTCUSDT, ETHUSDT, ...).
    -- LowCardinality dictionary-encodes them -> less disk, faster GROUP BY.
    symbol       LowCardinality(String),

    -- trade_id: Binance's per-trade id (field "t" in the trade stream). It is
    -- unique PER SYMBOL and increases over time. This is our DEDUP KEY: if the
    -- same trade is delivered/processed twice, both rows share (symbol, trade_id)
    -- and ReplacingMergeTree collapses them to one. UInt64 — ids grow large.
    trade_id     UInt64,

    -- price/quantity: Decimal64(8), NOT Float64 (Phase 3 correctness change).
    -- Money must be exact; floats can't represent every decimal and drift when
    -- summed. Decimal64(8) = 18 significant digits with 8 after the point —
    -- exactly Binance's max precision. Binance sends these as STRINGS, and the
    -- consumer parses the string straight into Decimal (no float in between).
    price        Decimal64(8),
    quantity     Decimal64(8),

    -- trade_time: when the trade happened, per Binance (field "T"), epoch ms,
    -- so DateTime64(3) = millisecond precision.
    trade_time   DateTime64(3, 'UTC'),

    -- ingested_at: when WE wrote the row (DEFAULT now64(3)). It doubles as the
    -- ReplacingMergeTree VERSION (see engine below): on a duplicate key, the row
    -- with the highest ingested_at wins. A reprocessed trade gets a newer
    -- timestamp, so the latest write is the one kept (data is identical anyway).
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- ReplacingMergeTree(version): like MergeTree, but during background part
-- merges it collapses rows that share the full ORDER BY key, keeping only the
-- one with the largest `version` (ingested_at here). That's how reprocessing
-- the same trades yields the same final result = effectively-once ingestion,
-- without needing true distributed exactly-once.
--
-- ⚠️ THE TRAP — dedup is at MERGE time, which is ASYNC/BACKGROUND, NOT at
-- insert. Right after a duplicate insert, BOTH rows exist until ClickHouse
-- merges those parts (could be seconds, could be much longer). So:
--     SELECT count() FROM trades            -> may include duplicates
--     SELECT count() FROM trades FINAL      -> dedup-correct (merge-on-read)
-- Any read that must be exact needs FINAL (or a GROUP BY that re-aggregates).
-- `OPTIMIZE TABLE trades FINAL` forces a merge now (fine for testing; don't run
-- it constantly in production — merges are meant to be lazy).
ENGINE = ReplacingMergeTree(ingested_at)

-- Partition by day: lets time-range queries prune to the relevant day(s) even
-- though the sort key is now (symbol, trade_id). Also easy retention later.
PARTITION BY toYYYYMMDD(trade_time)

-- ORDER BY = the dedup key. Must contain everything that identifies a unique
-- trade: (symbol, trade_id). (trade_id is monotonic per symbol, so this stays
-- roughly time-ordered too, and day-partition pruning handles time filters.)
ORDER BY (symbol, trade_id);
