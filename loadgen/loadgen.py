"""
Load generator for BENCHMARKING — publishes SYNTHETIC trades into Kafka at a
controllable rate. Binance's real feed is only tens of events/sec, far too low to
find the pipeline's ceiling, so we drive the load ourselves.

Run the REAL Binance producer SEPARATELY: stop it during a clean benchmark so the
measurements are pure synthetic load, not a mix.

Methodology baked into the design (this is what makes the numbers honest):
  * trade_time is stamped NOW, at publish time. So the consumer's
    (ingested_at - trade_time) measures end-to-end latency FROM KAFKA ENTRY to
    landing in ClickHouse — it excludes Binance's external network, which we don't
    control and shouldn't take credit/blame for.
  * trade_id is unique per run (time-based base + counter), so events do NOT
    collapse in ReplacingMergeTree. We want to measure REAL insert work, not dedup
    making rows disappear. (A separate DUP_MODE run can measure dedup overhead.)
  * this producer is tuned for raw THROUGHPUT (acks=1, big buffers) — deliberately
    different from the real producer's durability config. Here we stress-test the
    consumer -> ClickHouse path; we're not testing producer delivery guarantees.
"""

import logging
import os
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,SOLUSDT").split(",") if s.strip()]

# Must match the producer's schema exactly — loadgen writes to the SAME topic, so
# it serializes under the same registered 'trades-value' subject. (Canonical copy:
# schemas/trade.avsc.)
TRADE_VALUE_SCHEMA = """
{
  "type": "record",
  "name": "Trade",
  "namespace": "crypto.trades",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "trade_id", "type": "long"},
    {"name": "price", "type": "string"},
    {"name": "quantity", "type": "string"},
    {"name": "trade_time", "type": "long"}
  ]
}
"""

RATE = float(os.environ.get("LOADTEST_RATE", "10000"))        # target events/sec
DURATION = float(os.environ.get("LOADTEST_DURATION", "60"))   # seconds to run
DUP_MODE = os.environ.get("LOADTEST_DUP_MODE", "0") == "1"    # resend same ids (measure dedup)
# When running SEVERAL loadgens in parallel (e.g. 2x 50k for 100k/s), give each a
# distinct INSTANCE so their trade_id ranges don't overlap — otherwise they'd
# collide and ReplacingMergeTree would dedup the overlap away, undercounting.
INSTANCE = int(os.environ.get("LOADTEST_INSTANCE", "0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Plausible price anchors so the synthetic data looks real (and Decimal-parses fine).
BASE_PRICES = {"BTCUSDT": 60000.0, "ETHUSDT": 3000.0, "BNBUSDT": 600.0, "XRPUSDT": 0.5, "SOLUSDT": 150.0}

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [loadgen] %(message)s")
log = logging.getLogger("loadgen")


def make_producer() -> Producer:
    """Throughput-tuned producer (NOT the durability config of the real producer).

    Big local buffers + 1MB batches + lz4 + acks=1 let us push hard. acks=1 (leader
    ack only, no replica wait) is fine here: we're measuring the consumer/ClickHouse
    path, and on a single broker acks=1 and acks=all are the same anyway.
    """
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "loadgen",
        "linger.ms": 20,                          # let batches build for efficiency
        "batch.size": 1048576,                    # 1 MiB batches
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 2000000,  # large in-flight buffer
        "queue.buffering.max.kbytes": 2097152,    # 2 GiB cap
    })


def main() -> None:
    producer = make_producer()
    # Avro-encode like the real producer (same schema, same 'trades-value' subject),
    # so synthetic load exercises the real serialization + deserialization path.
    schema_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serialize = AvroSerializer(schema_client, TRADE_VALUE_SCHEMA)
    n_symbols = len(SYMBOLS)
    # Unique, monotonic base so this run's ids never collide with real trades or a
    # previous run (UInt64 has ample room: ~1.75e9 * 1e8 << 1.8e19). The INSTANCE
    # term (1e15 apart) keeps parallel loadgens in disjoint id ranges.
    base_id = int(time.time()) * 100_000_000 + INSTANCE * 1_000_000_000_000_000

    log.info("starting load: instance=%d rate=%.0f/s duration=%.0fs symbols=%d dup_mode=%s",
             INSTANCE, RATE, DURATION, n_symbols, DUP_MODE)

    sent = 0
    start = time.monotonic()
    next_report = start + 5.0

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= DURATION:
            break

        # Pace to RATE: catch up to however many events SHOULD have been sent by now.
        # If we can't keep up (generator CPU-bound), this loop just runs flat-out and
        # the final "actual rate" tells the honest truth.
        target = int(RATE * elapsed)
        now_ms = int(time.time() * 1000)
        while sent < target:
            sym = SYMBOLS[sent % n_symbols]
            base = BASE_PRICES.get(sym, 100.0)
            # In dup mode, reuse a tiny id space so ReplacingMergeTree collapses them.
            tid = (base_id + (sent % 1000)) if DUP_MODE else (base_id + sent)
            payload = serialize(
                {
                    "symbol": sym,
                    "trade_id": tid,
                    "price": f"{base * (1 + random.uniform(-0.0015, 0.0015)):.8f}",
                    "quantity": f"{random.uniform(0.0001, 3.0):.8f}",
                    "trade_time": now_ms,        # stamped at publish = latency clock start
                },
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
            )
            try:
                producer.produce(KAFKA_TOPIC, key=sym, value=payload)
                sent += 1
            except BufferError:
                # Local queue full (broker slower than us): drain then retry.
                producer.poll(0.1)
        producer.poll(0)

        if time.monotonic() >= next_report:
            log.info("sent=%d (%.0f/s avg)", sent, sent / elapsed)
            next_report += 5.0

    log.info("flushing %d queued...", len(producer))
    remaining = producer.flush(60)
    total_elapsed = time.monotonic() - start
    log.info("DONE: sent=%d in %.1fs => %.0f events/s pushed (%d unflushed)",
             sent, total_elapsed, sent / total_elapsed, remaining)


if __name__ == "__main__":
    main()
