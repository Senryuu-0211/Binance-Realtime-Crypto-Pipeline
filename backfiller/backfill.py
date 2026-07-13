"""
Backfiller: close trade_id GAPS by re-fetching the missing trades from Binance.

WHY THIS EXISTS — the last hole in "no data loss":
  Our producer reads Binance over a WebSocket. A WS is FIRE-AND-FORGET at the
  source: if the socket drops for a few seconds (Binance caps connections at
  ~24h and drops on any blip), the trades that happened DURING the gap are never
  replayed to us. Kafka's acks/idempotence and ClickHouse's dedup only protect
  data once it is INSIDE the pipeline — they cannot recover an event we never
  received. So "no data loss" was only true from Kafka onward; the WS edge could
  still silently lose events.

  The healthcheck already DETECTS this: Binance stamps each trade with a
  per-symbol monotonic id (field "t"), so a jump 1000 -> 1003 means ids 1001,
  1002 were lost. That's the *detection* half. THIS service is the *remediation*
  half: it finds those gaps and pulls the exact missing trades back from
  Binance's REST history, making the pipeline SELF-HEALING.

WHY /historicalTrades AND NOT /aggTrades (this is the subtle, easy-to-get-wrong
part):
  /api/v3/historicalTrades returns RAW trades whose "id" is the SAME id space as
  the WebSocket "t" field — so a backfilled trade collapses onto the live one via
  our (symbol, trade_id) dedup key. /api/v3/aggTrades returns AGGREGATE trades
  with a DIFFERENT id ("a") that does NOT line up with "t"; backfilling from it
  would insert rows that never dedup against the live stream and corrupt counts.
  historicalTrades needs an API KEY in the X-MBX-APIKEY header (it's a
  MARKET_DATA endpoint — key required, but no request SIGNATURE / secret needed).

WHY BACKFILL IS SAFE TO RUN REPEATEDLY:
  crypto.trades is a ReplacingMergeTree keyed by (symbol, trade_id). Re-inserting
  a trade we already have is a no-op after merge (same key, newer version wins,
  data identical). So paging can overlap, cycles can repeat, two backfillers
  could even run at once — the result is idempotent. That safety is what lets us
  be aggressive about re-fetching without fear of double-counting.

MODES:
  * Loop (default): every BACKFILL_INTERVAL_SECONDS, scan the recent window for
    gaps and heal them automatically. Part of `docker compose up`.
  * Once (BACKFILL_ONCE=1): one scan-and-heal pass, then exit. Used by
    `make backfill` for a manual/demo run.
  * Idle: if no BINANCE_API_KEY is set, /historicalTrades can't be called, so the
    service logs a clear "disabled" line and idles harmlessly (detection via the
    healthcheck still works; only auto-remediation is off). This keeps
    `docker compose up` working for anyone who hasn't created an API key.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
import requests

# ---------------------------------------------------------------------------
# Config — env-tunable, same conventions as the rest of the stack.
# ---------------------------------------------------------------------------
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# Region note: api.binance.com is geo-blocked in some places (HTTP 451). Unlike
# the peak seeder, the open mirror data-api.binance.vision does NOT serve
# historicalTrades with an API key, so backfill needs the real api host (or a
# regional one you have a key for).
BINANCE_REST_BASE = os.environ.get("BINANCE_REST_BASE", "https://api.binance.com").rstrip("/")
# The API key is REQUIRED for /historicalTrades. Empty => service idles (see main).
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "").strip()

# How often to scan for gaps. Offset a little from the healthcheck's 5-min cadence
# isn't required (idempotent), but running slightly less often is fine — a gap
# doesn't need healing within seconds.
BACKFILL_INTERVAL_SECONDS = float(os.environ.get("BACKFILL_INTERVAL_SECONDS", "300"))
# Look back this many minutes for gaps. MUST be >= the healthcheck's gap window
# (default 10) so we heal everything it can flag; a little wider is safer because
# a gap near the window edge on one side is still fully visible here.
BACKFILL_WINDOW_MINUTES = int(os.environ.get("BACKFILL_WINDOW_MINUTES", "15"))
# Safety ceiling: if a single "gap" is larger than this, DON'T try to backfill it.
# A gap of millions isn't a brief WS drop — it's usually a cold-start artifact
# (table just started; the first ids we saw aren't really preceded by a hole) or
# a very long outage better handled deliberately. We log it and skip so a runaway
# scan can't hammer the REST API. Raise it if you truly need to heal a big outage.
BACKFILL_MAX_GAP = int(os.environ.get("BACKFILL_MAX_GAP", "500000"))
# Binance allows up to 1000 trades per historicalTrades call.
REST_LIMIT = 1000
# Politeness sleep between REST pages so we stay well under the IP weight limit
# (historicalTrades is weight 25; the spot limit is ~6000/min). We also honor
# 429/Retry-After dynamically, but a small fixed pause avoids ever getting close.
REST_PAGE_PAUSE_SECONDS = float(os.environ.get("BACKFILL_PAGE_PAUSE_SECONDS", "0.3"))

BACKFILL_ONCE = os.environ.get("BACKFILL_ONCE", "0").lower() in ("1", "true", "yes")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "trades")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Columns for the trades insert — identical to the consumer's, so a backfilled
# row is byte-for-byte the same shape as a live one (ingested_at defaults + is
# the dedup version).
TRADE_COLUMNS = ["symbol", "trade_id", "price", "quantity", "trade_time"]
# Columns for the audit trail we write into the SAME health_checks table the
# healthcheck uses, so Grafana can show remediation next to detection.
HEALTH_COLUMNS = ["check_name", "symbol", "status", "value"]

# Mirror the trades DDL (source of truth: clickhouse/init/01_schema.sql) so the
# service is self-sufficient on an already-running ClickHouse. Idempotent.
DDL_TRADES = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
(
    symbol       LowCardinality(String),
    trade_id     UInt64,
    price        Decimal64(8),
    quantity     Decimal64(8),
    trade_time   DateTime64(3, 'UTC'),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_id)
"""
# Mirror the health DDL (source of truth: clickhouse/init/03_health.sql).
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

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [backfiller] %(message)s")
log = logging.getLogger("backfiller")

_running = True


def connect_clickhouse():
    """Connect + ensure both tables exist, retrying until ClickHouse is up."""
    delay = 1.0
    while _running:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            )
            client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
            client.command(DDL_TRADES)
            client.command(DDL_HEALTH)
            log.info("connected to ClickHouse %s:%s; tables ready", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
            return client
        except Exception as exc:  # noqa: BLE001
            log.warning("ClickHouse not ready (%s); retrying in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
    sys.exit(0)


def find_gaps(client) -> list:
    """Return the concrete gap RANGES per symbol in the recent window.

    The healthcheck records only a COUNT of missing ids; to actually re-fetch
    them we need the exact boundaries. Same window-function idea, but instead of
    summing the holes we emit, for each jump, the first and last missing id:

        received ids ...1000, 1003...  ->  gap_start=1001, gap_end=1002

    HOW:
      1. DISTINCT (symbol, trade_id) in the window — drop not-yet-merged dup rows.
      2. lagInFrame(trade_id) = the previous received id within the same symbol,
         ordered by id. The first row per symbol has no predecessor, so lagInFrame
         defaults to the id itself -> difference 0 -> never a false gap at the
         window's left edge (we don't invent a hole before the earliest id we saw).
      3. Where (id - prev_id) > 1 there's a hole: gap_start = prev_id + 1,
         gap_end = id - 1.

    Returns [(symbol, gap_start, gap_end, missing_count), ...], smallest id first.
    """
    in_list = ", ".join("'" + s.replace("'", "") + "'" for s in SYMBOLS)
    query = (
        f"SELECT symbol, prev_id + 1 AS gap_start, trade_id - 1 AS gap_end, "
        f"       (trade_id - prev_id - 1) AS missing "
        f"FROM ("
        f"  SELECT symbol, trade_id, "
        f"    lagInFrame(trade_id, 1, trade_id) "
        f"      OVER (PARTITION BY symbol ORDER BY trade_id) AS prev_id "
        f"  FROM ("
        f"    SELECT DISTINCT symbol, trade_id "
        f"    FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} "
        f"    WHERE symbol IN ({in_list}) "
        f"      AND trade_time > now() - INTERVAL {BACKFILL_WINDOW_MINUTES} MINUTE"
        f"  )"
        f") "
        f"WHERE trade_id - prev_id > 1 "
        f"ORDER BY symbol, gap_start"
    )
    return [
        (sym, int(gap_start), int(gap_end), int(missing))
        for sym, gap_start, gap_end, missing in client.query(query).result_rows
    ]


def fetch_page(session: requests.Session, symbol: str, from_id: int) -> list:
    """Fetch up to REST_LIMIT raw trades for `symbol` starting at id `from_id`.

    Returns Binance's list of trade dicts (ascending by id). Handles the two
    rate-limit responses Binance uses:
      429 Too Many Requests -> we're going too fast; sleep Retry-After and retry.
      418 I'm a teapot      -> our IP is temporarily banned for ignoring 429s;
                               back off hard. (We shouldn't hit this given our
                               pacing, but we handle it so a bad run degrades
                               gracefully instead of crash-looping.)
    """
    url = f"{BINANCE_REST_BASE}/api/v3/historicalTrades"
    params = {"symbol": symbol, "fromId": from_id, "limit": REST_LIMIT}
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    for attempt in range(5):
        resp = session.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code in (429, 418):
            wait = float(resp.headers.get("Retry-After", 5))
            log.warning("%s: rate-limited (HTTP %d); backing off %.0fs", symbol, resp.status_code, wait)
            time.sleep(wait)
            continue
        if resp.status_code == 451:
            raise RuntimeError(
                f"HTTP 451 (geo-blocked) from {BINANCE_REST_BASE}. historicalTrades "
                "needs an api host you can reach WITH your API key."
            )
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"{symbol}: gave up after repeated rate-limit responses")


def rows_from_trades(symbol: str, trades: list, lo: int, hi: int) -> list:
    """Map Binance REST trade dicts onto trades-table rows, keeping only ids in
    [lo, hi] (the gap we're healing).

    REST field -> column:
      id    -> trade_id     (SAME id space as the WS "t" = dedup key)
      price -> price         (Decimal straight from the string; no float)
      qty   -> quantity
      time  -> trade_time    (epoch ms -> tz-aware datetime, like the consumer)
    """
    out = []
    for t in trades:
        tid = int(t["id"])
        if tid < lo or tid > hi:
            continue
        trade_time = datetime.fromtimestamp(t["time"] / 1000.0, tz=timezone.utc)
        out.append([symbol, tid, Decimal(str(t["price"])), Decimal(str(t["qty"])), trade_time])
    return out


def backfill_gap(client, session: requests.Session, symbol: str, gap_start: int, gap_end: int) -> int:
    """Page /historicalTrades from gap_start to gap_end, inserting the missing
    trades. Returns how many rows were inserted.

    Paging: historicalTrades?fromId=X returns ids ascending from X. We advance
    the cursor to (last id in page)+1 until we pass gap_end or a page returns no
    forward progress (guards against an infinite loop on a weird response).
    """
    inserted = 0
    cursor = gap_start
    # A gap spans at most (gap_end-gap_start+1) ids; bound pages generously.
    max_pages = (gap_end - gap_start) // REST_LIMIT + 2
    for _ in range(max_pages):
        if cursor > gap_end or not _running:
            break
        page = fetch_page(session, symbol, cursor)
        if not page:
            log.warning("%s: empty page at fromId=%d (gap %d-%d) — stopping", symbol, cursor, gap_start, gap_end)
            break
        rows = rows_from_trades(symbol, page, gap_start, gap_end)
        if rows:
            client.insert(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}", rows, column_names=TRADE_COLUMNS)
            inserted += len(rows)
        last_id = int(page[-1]["id"])
        if last_id < cursor:      # no forward progress — bail rather than spin
            break
        cursor = last_id + 1
        time.sleep(REST_PAGE_PAUSE_SECONDS)
    return inserted


def record_audit(client, rows: list) -> None:
    """Write backfill activity into health_checks so Grafana shows remediation.

    check_name='backfill'. One summary row per cycle (symbol='ALL') plus one row
    per symbol that was healed. value = number of trades backfilled.
    """
    try:
        client.insert(f"{CLICKHOUSE_DB}.health_checks", rows, column_names=HEALTH_COLUMNS)
    except Exception as exc:  # noqa: BLE001 — auditing must never kill the healer
        log.error("failed to write backfill audit row: %s", exc)


def run_once(client, session: requests.Session) -> int:
    """One scan-and-heal pass. Returns total trades backfilled this cycle."""
    gaps = find_gaps(client)
    if not gaps:
        record_audit(client, [["backfill", "ALL", "OK", 0.0]])
        log.info("no gaps in the last %dmin — nothing to backfill", BACKFILL_WINDOW_MINUTES)
        return 0

    total = 0
    per_symbol: dict[str, int] = {}
    for symbol, gap_start, gap_end, missing in gaps:
        if missing > BACKFILL_MAX_GAP:
            log.warning("%s: gap %d-%d is %d ids (> BACKFILL_MAX_GAP=%d) — skipping as likely "
                        "cold-start/large-outage, not a WS blip", symbol, gap_start, gap_end,
                        missing, BACKFILL_MAX_GAP)
            continue
        log.info("%s: healing gap %d-%d (%d missing)", symbol, gap_start, gap_end, missing)
        try:
            got = backfill_gap(client, session, symbol, gap_start, gap_end)
        except Exception as exc:  # noqa: BLE001 — one bad gap shouldn't stop the rest
            log.error("%s: backfill of gap %d-%d failed: %s", symbol, gap_start, gap_end, exc)
            continue
        total += got
        per_symbol[symbol] = per_symbol.get(symbol, 0) + got
        log.info("%s: backfilled %d/%d trades for gap %d-%d", symbol, got, missing, gap_start, gap_end)

    audit = [["backfill", "ALL", "BACKFILLED" if total else "OK", float(total)]]
    audit += [["backfill", sym, "BACKFILLED", float(cnt)] for sym, cnt in per_symbol.items()]
    record_audit(client, audit)
    log.info("cycle done — backfilled %d trade(s) across %d symbol(s)", total, len(per_symbol))
    return total


def main() -> None:
    if not SYMBOLS:
        log.error("SYMBOLS is empty")
        sys.exit(1)

    client = connect_clickhouse()

    # No API key => we cannot call historicalTrades. Don't crash-loop; record the
    # state once (so it's visible in Grafana) and idle. Detection still runs in
    # the healthcheck; only AUTO-remediation is off until a key is provided.
    if not BINANCE_API_KEY:
        log.warning("BINANCE_API_KEY not set — backfill DISABLED (gap detection still runs in the "
                    "healthcheck). Set BINANCE_API_KEY to enable automatic gap healing.")
        record_audit(client, [["backfill", "ALL", "DISABLED", 0.0]])
        if BACKFILL_ONCE:
            return
        while _running:                       # idle harmlessly; keep the container alive
            time.sleep(min(BACKFILL_INTERVAL_SECONDS, 60.0))
        return

    session = requests.Session()
    mode = "once" if BACKFILL_ONCE else f"loop every {BACKFILL_INTERVAL_SECONDS:.0f}s"
    log.info("backfiller started (%s, window=%dmin, host=%s)", mode, BACKFILL_WINDOW_MINUTES, BINANCE_REST_BASE)

    if BACKFILL_ONCE:
        run_once(client, session)
        return

    while _running:
        try:
            run_once(client, session)
        except Exception as exc:  # noqa: BLE001 — a bad cycle must not kill the healer
            log.error("backfill cycle failed: %s", exc)
            try:
                client = connect_clickhouse()   # rebuild a possibly-dead connection
            except SystemExit:
                break
        # Sleep in short slices so SIGTERM stops us promptly, not after a full interval.
        slept = 0.0
        while _running and slept < BACKFILL_INTERVAL_SECONDS:
            time.sleep(min(5.0, BACKFILL_INTERVAL_SECONDS - slept))
            slept += 5.0


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
