"""
Seed all-time PEAK prices into crypto.peak_prices from Binance history.

Why this exists: the materialized view (peak_prices_mv) only ever sees trades
that arrive AFTER it's running. So without seeding, "all-time peak" really means
"max since the pipeline started" — which for a coin down from its highs is just
wrong (it'd report today's high as the all-time high). We fix that by seeding
the real historical high per symbol from Binance's REST API, ONCE, up front.
The live MV then keeps the peak up to date from there.

How the peak stays correct after seeding:
  seeded historical high  ─┐
                           ├─►  maxMerge() takes the LARGER  ─►  true all-time peak
  live max from the MV    ─┘
Because the column is a max() aggregate state, inserting a seed value never
lowers an existing (higher) live peak — max always wins. That also makes this
script safely idempotent: re-running re-inserts the same/refreshed historical
high; the result only ever moves up, never duplicates into a wrong number.

Run it with:  make seed-peaks   (= docker compose run --rm --build seeder)
"""

import logging
import os
import sys
import time

import clickhouse_connect
import requests

# ---------------------------------------------------------------------------
# Config — same SYMBOLS the producer uses, so peaks track the live coins.
# ---------------------------------------------------------------------------
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# Region note: api.binance.com is geo-blocked in some places (HTTP 451). If so,
# set BINANCE_REST_BASE=https://data-api.binance.vision in .env (same data, open).
BINANCE_REST_BASE = os.environ.get("BINANCE_REST_BASE", "https://api.binance.com").rstrip("/")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Binance kline = array; the HIGH of the candle is at index 2.
# [ openTime, open, HIGH, low, close, volume, closeTime, ... ]
KLINE_HIGH_INDEX = 2

# Mirrors clickhouse/init/02_peak.sql so the seeder also works on an
# already-running ClickHouse where the init script never re-ran. Idempotent.
DDL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.peak_prices
(
    symbol     LowCardinality(String),
    peak_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY symbol
"""
DDL_MV = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.peak_prices_mv
TO {CLICKHOUSE_DB}.peak_prices
AS SELECT symbol, maxState(toFloat64(price)) AS peak_state
FROM {CLICKHOUSE_DB}.trades
GROUP BY symbol
"""

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [seeder] %(message)s")
log = logging.getLogger("seeder")


def connect_clickhouse():
    """Connect with retry (ClickHouse may still be coming up) and ensure schema."""
    delay = 1.0
    for _ in range(20):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            )
            client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
            client.command(DDL_TABLE)   # safety net for already-running instances
            client.command(DDL_MV)
            log.info("connected to ClickHouse %s:%s; peak schema ready", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
            return client
        except Exception as exc:  # noqa: BLE001
            log.warning("ClickHouse not ready (%s); retrying in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
    log.error("could not connect to ClickHouse")
    sys.exit(1)


def fetch_all_time_high(session: requests.Session, symbol: str) -> float | None:
    """Return the all-time high for `symbol` from MONTHLY candles.

    interval=1M + limit=1000 returns every month since the coin listed (far more
    than exist). Each candle's HIGH is the highest price in that month (intraday
    spikes included), so max(high) across all months IS the true all-time high.
    """
    url = f"{BINANCE_REST_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": "1M", "limit": 1000}
    resp = session.get(url, params=params, timeout=15)
    if resp.status_code == 451:
        raise RuntimeError(
            f"HTTP 451 (geo-blocked) from {BINANCE_REST_BASE}. "
            "Set BINANCE_REST_BASE=https://data-api.binance.vision in .env and retry."
        )
    resp.raise_for_status()
    klines = resp.json()
    if not klines:
        log.warning("%s: no klines returned (invalid symbol?) — skipping", symbol)
        return None
    # float() on the HIGH string of each candle; take the max. If we ever get a
    # tiny number here, it means we're reading the wrong array index.
    return max(float(k[KLINE_HIGH_INDEX]) for k in klines)


def seed(client, peaks: dict[str, float]) -> None:
    """Insert one max-STATE per symbol in a single statement, then compact.

    We build:  INSERT INTO peak_prices SELECT symbol, maxState(high) FROM (
                 SELECT 'BTCUSDT' symbol, toFloat64(109588.0) high
                 UNION ALL SELECT 'ETHUSDT', toFloat64(4878.26) ...
               ) GROUP BY symbol
    maxState() turns each plain number into the AggregateFunction(max) state the
    column expects. (You can't INSERT a raw Float64 into a State column.)
    """
    rows = " UNION ALL ".join(
        f"SELECT '{sym}' AS symbol, toFloat64({high}) AS high"
        for sym, high in peaks.items()
    )
    client.command(
        f"INSERT INTO {CLICKHOUSE_DB}.peak_prices (symbol, peak_state) "
        f"SELECT symbol, maxState(high) FROM ({rows}) GROUP BY symbol"
    )
    # Collapse the freshly-inserted state into ONE row per symbol so repeated
    # seeding never leaves a growing pile of unmerged parts. (Background merges
    # would do this eventually; FINAL forces it now, keeping the table tidy.)
    client.command(f"OPTIMIZE TABLE {CLICKHOUSE_DB}.peak_prices FINAL")


def main() -> None:
    if not SYMBOLS:
        log.error("SYMBOLS is empty")
        sys.exit(1)

    client = connect_clickhouse()
    session = requests.Session()

    peaks: dict[str, float] = {}
    for symbol in SYMBOLS:
        try:
            high = fetch_all_time_high(session, symbol)
        except Exception as exc:  # noqa: BLE001 — one bad symbol shouldn't kill the run
            log.error("%s: failed to fetch history: %s", symbol, exc)
            continue
        if high is not None:
            peaks[symbol] = high
            log.info("%s: all-time high = %.8g", symbol, high)

    if not peaks:
        log.error("no peaks fetched — nothing seeded (check region / BINANCE_REST_BASE)")
        sys.exit(1)

    seed(client, peaks)

    # Read it back the CORRECT way (maxMerge + GROUP BY) to confirm.
    result = client.query(
        f"SELECT symbol, maxMerge(peak_state) AS peak "
        f"FROM {CLICKHOUSE_DB}.peak_prices GROUP BY symbol ORDER BY symbol"
    )
    log.info("seeded peaks now in ClickHouse:")
    for sym, peak in result.result_rows:
        log.info("   %-10s %.8g", sym, peak)
    log.info("done — seeded %d symbol(s)", len(peaks))


if __name__ == "__main__":
    main()
