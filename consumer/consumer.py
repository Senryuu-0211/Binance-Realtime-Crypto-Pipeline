"""
Consumer: Redpanda topic  ->  (batched)  ->  ClickHouse.

The one idea to absorb here is WHY WE BATCH.

ClickHouse stores a MergeTree table as immutable "parts" (folders of column
files). Every INSERT creates at least one new part, and a background thread
continuously merges parts together. If we inserted one row per trade, we'd
create thousands of tiny parts per minute -> merge storms, wasted CPU/IO, and
eventually the dreaded "TOO_MANY_PARTS" error that blocks inserts entirely.

So we accumulate rows and write them in one shot: flush when we've gathered
BATCH_SIZE rows OR FLUSH_INTERVAL_SECONDS have passed — whichever comes first.
That bounds both part count and worst-case latency.

Delivery semantics = AT-LEAST-ONCE: auto-commit is OFF, and we commit Kafka
offsets only AFTER a batch is safely in ClickHouse. If we crash mid-batch we
re-read the uncommitted messages on restart (a few duplicate rows possible —
acceptable for Phase 1; exactly-once comes later).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer

# ---------------------------------------------------------------------------
# Configuration — all from environment (see .env / docker-compose.yml).
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "clickhouse-writer")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "trades")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
FLUSH_INTERVAL_SECONDS = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "2"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Columns we write. ingested_at is intentionally omitted — ClickHouse fills it
# from the table's DEFAULT now64(3) at insert time.
COLUMNS = ["symbol", "price", "quantity", "trade_time"]

# Safety net: mirrors clickhouse/init/01_schema.sql. The init script is the
# source of truth, but running this idempotent DDL on startup means the
# consumer works even if it raced ahead of init, or the volume predates it.
DDL_DATABASE = f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}"
DDL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
(
    symbol       LowCardinality(String),
    price        Float64,
    quantity     Float64,
    trade_time   DateTime64(3, 'UTC'),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_time)
"""

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [consumer] %(message)s")
log = logging.getLogger("consumer")

_running = True  # flipped to False by the signal handlers for a clean exit


def connect_clickhouse():
    """Connect to ClickHouse, retrying until it's reachable.

    On a fresh `docker compose up`, ClickHouse may still be applying its init
    SQL when we start, so we back off and retry rather than crash-looping.
    Connects WITHOUT a default database so we can CREATE DATABASE first.
    """
    delay = 1.0
    while _running:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            client.command(DDL_DATABASE)
            client.command(DDL_TABLE)
            log.info("connected to ClickHouse at %s:%s, table %s.%s ready",
                     CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB, CLICKHOUSE_TABLE)
            return client
        except Exception as exc:  # noqa: BLE001 — keep retrying through any startup error
            log.warning("ClickHouse not ready (%s); retrying in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
    sys.exit(0)  # got a shutdown signal while still trying to connect


def parse_message(value: bytes):
    """Turn one Kafka message into a ClickHouse row, or None if unusable.

    The producer sends: {"symbol","price","quantity","trade_time"(epoch ms)}.
    We convert trade_time ms -> a tz-aware datetime so clickhouse-connect maps
    it correctly onto DateTime64(3); passing a raw int would be read as seconds.
    """
    try:
        d = json.loads(value)
        trade_time = datetime.fromtimestamp(d["trade_time"] / 1000.0, tz=timezone.utc)
        return [str(d["symbol"]), float(d["price"]), float(d["quantity"]), trade_time]
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        log.warning("dropping bad message: %s", exc)
        return None


def insert_batch(client, rows):
    """Insert one batch, retrying transient failures (and reconnecting).

    Returns the (possibly new) client. Raises if it can't write after several
    tries — we deliberately let the process crash then: because we haven't
    committed offsets, `restart: unless-stopped` brings us back and we re-read
    the same messages (at-least-once).
    """
    delay = 1.0
    for attempt in range(1, 6):
        try:
            client.insert(CLICKHOUSE_TABLE, rows, column_names=COLUMNS, database=CLICKHOUSE_DB)
            return client
        except Exception as exc:  # noqa: BLE001
            log.warning("insert failed (attempt %d/5): %s", attempt, exc)
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
            try:
                client = connect_clickhouse()  # rebuild a possibly-dead connection
            except SystemExit:
                raise
    raise RuntimeError("ClickHouse insert failed repeatedly; crashing to force replay")


def make_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        # OFF on purpose: we commit ourselves, only after a successful insert.
        "enable.auto.commit": False,
        # First run (no committed offset): start at the beginning of the topic.
        "auto.offset.reset": "earliest",
        # ClickHouse stalls shouldn't get us kicked out of the consumer group.
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def main() -> None:
    _install_signal_handlers()
    client = connect_clickhouse()
    consumer = make_consumer()
    log.info("consuming '%s' as group '%s' (batch=%d, flush=%.1fs)",
             KAFKA_TOPIC, KAFKA_GROUP_ID, BATCH_SIZE, FLUSH_INTERVAL_SECONDS)

    total = 0
    try:
        while _running:
            # consume() IS our batching primitive: it returns as soon as either
            # BATCH_SIZE messages are available OR FLUSH_INTERVAL_SECONDS pass.
            # So "flush on size OR time" falls out for free.
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=FLUSH_INTERVAL_SECONDS)
            if not messages:
                continue  # idle tick — nothing to write

            rows = []
            for m in messages:
                if m.error():
                    # _PARTITION_EOF etc. are informational, not real failures.
                    log.debug("kafka info/err: %s", m.error())
                    continue
                row = parse_message(m.value())
                if row is not None:
                    rows.append(row)

            if rows:
                client = insert_batch(client, rows)
                total += len(rows)
                log.info("inserted %d rows (total %d)", len(rows), total)

            # Commit AFTER the data is durable in ClickHouse -> at-least-once.
            consumer.commit(asynchronous=False)
    finally:
        log.info("closing consumer...")
        consumer.close()
        log.info("bye (inserted %d rows total)", total)


def _install_signal_handlers() -> None:
    def _stop(_signum, _frame):
        global _running
        _running = False
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _stop)
        except (ValueError, OSError):
            pass  # not on main thread / unsupported — fine


if __name__ == "__main__":
    main()
