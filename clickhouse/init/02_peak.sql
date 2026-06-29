-- ============================================================================
-- Phase 2 Part B — all-time PEAK price per symbol.
--
-- Goal: a cheap, always-fresh "highest price ever seen" per coin, so Grafana can
-- show "current price as % of all-time peak". Two pieces:
--   1) peak_prices      — an AggregatingMergeTree table holding the running max
--   2) peak_prices_mv   — a materialized view that feeds it incrementally
--
-- Like 01_schema.sql, this runs ONCE on a fresh ClickHouse volume. On an
-- ALREADY-RUNNING instance it won't re-run, so the seeder (make seed-peaks)
-- also applies this same DDL idempotently. Everything is IF NOT EXISTS.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1) The peak table.
--
-- WHY NOT just `SELECT max(price) FROM trades GROUP BY symbol` at query time?
-- Because that scans the ENTIRE trades table every time, and gets slower and
-- slower as trades pile up (millions of rows → full scan on every dashboard
-- refresh). That cost grows without bound.
--
-- AggregatingMergeTree flips it around: we store a partial-aggregate STATE per
-- symbol and let ClickHouse merge states in the background. `max` is the
-- cheapest possible aggregate to maintain — its state is a single fixed-size
-- number, updated in O(1) per trade. A read touches only a few state rows
-- (one per symbol after merges), so query cost stays FLAT no matter how much
-- raw data accumulates.
--
-- The column type is AggregateFunction(max, Float64): not a Float64, but the
-- serialized *state* of a max() aggregation. You write it with maxState() and
-- read it with maxMerge() (see the trap note below).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS crypto.peak_prices
(
    symbol     LowCardinality(String),
    peak_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY symbol;                 -- one logical state row per symbol; merges collapse them

-- ---------------------------------------------------------------------------
-- 2) The materialized view that maintains the peak.
--
-- A ClickHouse MV is NOT a cached snapshot that refreshes. It's an INSERT-TIME
-- TRIGGER: each time a block of rows is inserted into crypto.trades, this
-- SELECT runs OVER THAT BLOCK ONLY and its result is inserted into the target
-- table (peak_prices). It never sees history, only the block flowing in.
--
-- That "only the current block" limitation is FINE here, because an all-time
-- max is monotonic: max(everything) = max( max(block_1), max(block_2), ... ).
-- Each block contributes its own maxState; AggregatingMergeTree merges them by
-- taking the larger. So incremental + per-block is exactly correct for max.
-- (This would NOT be correct for something needing history, e.g. a rolling
--  average — that must be a windowed query straight over trades.)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.peak_prices_mv
TO crypto.peak_prices                 -- write results into the table above
AS
SELECT
    symbol,
    maxState(price) AS peak_state     -- partial max state for THIS inserted block
FROM crypto.trades
GROUP BY symbol;

-- ---------------------------------------------------------------------------
-- ⚠️ READING THE PEAK — the #1 trap with AggregatingMergeTree:
--
--   WRONG:  SELECT symbol, peak_state FROM crypto.peak_prices;
--           -> returns raw, unmerged STATE blobs (garbage as a number, and
--              there may be several rows per symbol that haven't merged yet).
--
--   RIGHT:  SELECT symbol, maxMerge(peak_state) AS peak
--           FROM crypto.peak_prices GROUP BY symbol;
--           -> the -Merge combinator + GROUP BY combines all states into the
--              final number. (Equivalently: SELECT ... FROM peak_prices FINAL.)
--
-- Forgetting -Merge doesn't error — it silently returns wrong values. Always
-- pair the State-typed column with maxMerge(...) + GROUP BY when you read it.
-- ---------------------------------------------------------------------------
