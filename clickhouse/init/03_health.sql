-- ============================================================================
-- Phase 3 (ops) — pipeline HEALTH CHECK results.
--
-- A streaming pipeline needs ACTIVE monitoring: you want to catch a stall before
-- a stakeholder notices the dashboard froze. The healthcheck service runs checks
-- on a schedule and appends the results here; Grafana reads this table to SHOW
-- health and to ALERT on it. (For Bosch/industrial this is the same shape as
-- monitoring a sensor-telemetry stream for dropouts.)
--
-- This is an append-only LOG of check results — one row per check per run.
-- Runs once on a fresh volume; the healthcheck service also creates it
-- idempotently on startup (so it works on an already-running ClickHouse).
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto.health_checks
(
    check_name  LowCardinality(String),   -- 'freshness' (end-to-end lag) | 'symbol_stall'
    symbol      LowCardinality(String),   -- 'ALL' for the overall check, or the coin
    status      LowCardinality(String),   -- 'OK' | 'STALE' | 'STALLED'
    value       Float64,                  -- the measured age/lag, in SECONDS
    checked_at  DateTime64(3, 'UTC') DEFAULT now64(3)   -- when the check ran
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(checked_at)
ORDER BY (check_name, symbol, checked_at)
-- Bound growth: this is monitoring data, not a system of record. 30 days is
-- plenty to chart trends; ClickHouse drops older parts automatically.
TTL toDateTime(checked_at) + INTERVAL 30 DAY;
