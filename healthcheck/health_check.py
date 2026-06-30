"""
Scheduled pipeline health check.

WHY THIS EXISTS: dashboards are PASSIVE — they only help if a human is looking.
A production streaming pipeline needs ACTIVE monitoring that notices a stall on
its own and raises an alert, before a stakeholder notices stale numbers. (Same
shape as watching an industrial sensor-telemetry stream for dropouts.)

WHY A SIMPLE LOOP, NOT AIRFLOW: this is ONE periodic job with no dependency
graph. A 5-minute `while True: check(); sleep()` in a tiny container is the right
amount of machinery. Airflow (scheduler + webserver + metadata DB + workers) earns
its weight when you have multi-step DAGs with dependencies/retries/backfills —
reach for it then, not for a single recurring check.

WHY WRITE RESULTS TO CLICKHOUSE (not call Slack/PagerDuty here): we keep alerting
IN-STACK — the check just records status rows, and Grafana (already in the stack)
reads them to display health AND to fire alerts. No external integrations / SMTP
to configure. In real production you'd route Grafana alerts onward to PagerDuty
or Slack; that's a contact-point change, not a code change.

The two checks (and why both):
  - FRESHNESS (overall): now() - newest trade_time. Catches a total stall — the
    whole pipeline stopped writing.
  - PER-SYMBOL STALL: last trade age for EACH symbol. Catches a PARTIAL failure
    the overall check would miss — e.g. BTC keeps flowing (so overall freshness
    looks fine) but XRP's subscription silently dropped. Overall max() is
    dominated by the busiest symbol, so only a per-symbol view sees this.
"""

import logging
import os
import sys
import time

import clickhouse_connect

# ---------------------------------------------------------------------------
# Config — thresholds are env-tunable so you can tighten/loosen without a rebuild.
# ---------------------------------------------------------------------------
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

CHECK_INTERVAL_SECONDS = float(os.environ.get("CHECK_INTERVAL_SECONDS", "300"))          # run every 5 min
FRESHNESS_THRESHOLD_SECONDS = float(os.environ.get("FRESHNESS_THRESHOLD_SECONDS", "120"))  # overall stale > 2 min
# Per-symbol stall threshold is LOOSER than overall freshness on purpose: a single
# low-volume coin naturally has longer gaps between trades than BTC, so too tight a
# value would false-alarm at quiet times. 5 min is safe for the liquid majors we
# track; raise it if you add a thin-liquidity symbol.
SYMBOL_STALL_THRESHOLD_SECONDS = float(os.environ.get("SYMBOL_STALL_THRESHOLD_SECONDS", "300"))

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

HEALTH_COLUMNS = ["check_name", "symbol", "status", "value"]   # checked_at = DEFAULT now64(3)

# Mirrors clickhouse/init/03_health.sql so the service is self-sufficient on an
# already-running ClickHouse where the init script didn't re-run.
DDL_HEALTH = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.health_checks
(
    check_name  LowCardinality(String),
    symbol      LowCardinality(String),
    status      LowCardinality(String),
    value       Float64,
    checked_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(checked_at)
ORDER BY (check_name, symbol, checked_at)
TTL toDateTime(checked_at) + INTERVAL 30 DAY
"""

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [healthcheck] %(message)s")
log = logging.getLogger("healthcheck")

_running = True


def connect():
    """Connect + ensure the health table exists, retrying until ClickHouse is up."""
    delay = 1.0
    while _running:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            )
            client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
            client.command(DDL_HEALTH)
            log.info("connected to ClickHouse %s:%s; health table ready", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
            return client
        except Exception as exc:  # noqa: BLE001
            log.warning("ClickHouse not ready (%s); retrying in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
    sys.exit(0)


def check_freshness(client) -> list:
    """Overall end-to-end freshness: seconds since the newest trade.

    We compute the lag with ClickHouse's own now() so there's no clock skew
    between this container and the data. max(trade_time) is dedup-safe on the
    ReplacingMergeTree (duplicates can't change a max), so no FINAL needed.
    """
    row = client.query(
        f"SELECT count() AS n, "
        f"       ifNull(dateDiff('second', max(trade_time), now()), 999999) AS lag_s "
        f"FROM {CLICKHOUSE_DB}.trades"
    ).first_row
    n, lag = row[0], float(row[1])
    if n == 0:
        # No data at all yet — treat as stale so it's visible, not silently green.
        return ["freshness", "ALL", "STALE", lag]
    status = "STALE" if lag > FRESHNESS_THRESHOLD_SECONDS else "OK"
    return ["freshness", "ALL", status, lag]


def check_symbol_stalls(client) -> list:
    """Per-symbol last-trade age, for EACH configured symbol.

    Returns one row per symbol. A symbol with NO rows at all (never seen) reports
    a huge age + STALLED, so a fully-dead stream isn't silently absent.
    """
    # Build the IN-list from the configured symbols. They come from our own env
    # (operator-controlled), and we strip any quote defensively.
    in_list = ", ".join("'" + s.replace("'", "") + "'" for s in SYMBOLS)
    rows = client.query(
        f"SELECT symbol, dateDiff('second', max(trade_time), now()) AS age_s "
        f"FROM {CLICKHOUSE_DB}.trades WHERE symbol IN ({in_list}) GROUP BY symbol"
    ).result_rows
    age_by_symbol = {sym: float(age) for sym, age in rows}

    out = []
    for sym in SYMBOLS:
        age = age_by_symbol.get(sym, 999999.0)   # missing entirely = never seen
        status = "STALLED" if age > SYMBOL_STALL_THRESHOLD_SECONDS else "OK"
        out.append(["symbol_stall", sym, status, age])
    return out


def run_once(client) -> None:
    """One cycle: run both checks, write all result rows in a single insert."""
    rows = [check_freshness(client)]
    rows.extend(check_symbol_stalls(client))
    client.insert(f"{CLICKHOUSE_DB}.health_checks", rows, column_names=HEALTH_COLUMNS)

    # Log a one-line summary so `docker compose logs healthcheck` is readable.
    summary = ", ".join(f"{r[1]}={r[2]}({r[3]:.0f}s)" for r in rows)
    bad = [r for r in rows if r[2] != "OK"]
    (log.warning if bad else log.info)("health: %s", summary)


def main() -> None:
    client = connect()
    log.info("health checks every %.0fs (freshness>%.0fs=STALE, per-symbol>%.0fs=STALLED)",
             CHECK_INTERVAL_SECONDS, FRESHNESS_THRESHOLD_SECONDS, SYMBOL_STALL_THRESHOLD_SECONDS)
    while _running:
        try:
            run_once(client)
        except Exception as exc:  # noqa: BLE001 — a check failure must NOT kill the monitor
            # ClickHouse blip etc.: log and try again next cycle (rebuild client in case
            # the connection went bad). The monitor staying alive is the whole point.
            log.error("health check cycle failed: %s", exc)
            try:
                client = connect()
            except SystemExit:
                break
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    import signal

    def _stop(_s, _f):
        global _running
        _running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _stop)
        except (ValueError, OSError):
            pass
    main()
