"""
Producer: Binance live trades  ->  Kafka topic.

What it does, end to end:
  1. Opens a WebSocket to Binance's public trade stream for the configured symbols.
  2. For every trade tick, publishes a small JSON event to the Kafka topic,
     keyed by symbol.
  3. Survives the disconnects Binance WILL throw at us (it caps connections at
     ~24h and drops sockets on any network blip) by reconnecting with backoff.

The code is commented to TEACH, not just to run. The two ideas worth absorbing:
  * why the reconnect loop is shaped the way it is (see connect_and_stream), and
  * why confluent-kafka's produce() is fire-and-forget (see publish / poll).
"""

import asyncio
import json
import logging
import os
import random
import signal

import websockets
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration — all from environment (see .env / docker-compose.yml).
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
BINANCE_WS_BASE = os.environ.get("BINANCE_WS_BASE", "wss://stream.binance.com:9443")
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Reconnect/backoff tuning.
BACKOFF_BASE = 1.0     # first retry waits ~1s
BACKOFF_CAP = 30.0     # never wait longer than 30s between retries
# If no message arrives within this many seconds we assume the socket is dead
# and force a reconnect. BTC/ETH trade many times per second, so a long silence
# means something is wrong (Binance pings every ~3 min, ticks far more often).
RECV_TIMEOUT = 30.0

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [producer] %(message)s")
log = logging.getLogger("producer")


def build_stream_url() -> str:
    """Binance 'combined streams' URL: one socket carrying all our symbols.

    e.g. wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    Stream names must be lowercase. Each message is wrapped as
    {"stream": "...", "data": {...the trade...}}.
    """
    streams = "/".join(f"{s.lower()}@trade" for s in SYMBOLS)
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


# Running tally of delivery outcomes so failures are OBSERVABLE, not silent.
STATS = {"delivered": 0, "failed": 0}


def on_delivery(err, msg):
    """Called (by producer.poll / flush) once the broker acknowledges — or, after
    all retries, finally rejects — a message. produce() returns immediately; THIS
    callback is where we learn the REAL outcome. We count both outcomes so a
    failure shows up in logs/metrics instead of vanishing into "fire and forget".
    """
    if err is not None:
        STATS["failed"] += 1
        log.error("delivery FAILED (total failed=%d): %s", STATS["failed"], err)
    else:
        STATS["delivered"] += 1


def make_producer() -> Producer:
    """confluent-kafka Producer config — tuned so messages are NOT silently lost
    in the handoff to Kafka.

    produce() is fire-and-QUEUE: it copies the record into librdkafka's internal
    buffer and a background thread sends it. These settings make that handoff
    reliable and make failures visible.
    """
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "binance-producer",
        # Wait up to 50ms to batch messages together -> fewer, bigger requests.
        "linger.ms": 50,
        # Cheap CPU for less network: compress batches.
        "compression.type": "lz4",

        # --- Durability / no-loss settings -----------------------------------
        # acks=all: the broker acks ONLY after the record is persisted to all
        # in-sync replicas. On our single broker that's 1 replica, but it's the
        # correct setting that stays safe as the cluster grows. (acks=1 acks
        # before replication; acks=0 doesn't wait at all = trivially lossy.)
        "acks": "all",

        # enable.idempotence: the broker stamps each (producer, partition) record
        # with a sequence number and DROPS duplicates from the producer's OWN
        # retries. Without it, retrying after a lost ack writes the message twice;
        # with it, internal retries are exactly-once onto the partition. librdkafka
        # auto-enforces the prerequisites (acks=all, retries>0, max.in.flight<=5),
        # so we don't fight it by hand — we just keep acks=all explicit.
        "enable.idempotence": True,

        # delivery.timeout.ms: the TOTAL budget to get a record acked, including
        # all retries. If it can't make it in this window, the delivery callback
        # fires with an error — so the message is REPORTED as failed, never
        # silently dropped. (Retry count is managed by idempotence under this cap.)
        "delivery.timeout.ms": 120000,   # keep retrying for 2 min, then report
    })


def publish(producer: Producer, symbol: str, trade_id: int, price: str, quantity: str, trade_time_ms: int) -> None:
    """Queue one trade event. Keyed by symbol so all of a symbol's trades land
    on the same partition (preserves per-symbol ordering).

    price/quantity are forwarded as the RAW STRINGS Binance sent (e.g.
    "64123.45000000"), NOT floats. JSON has no decimal type, so a float here
    would already lose precision before the consumer can build a Decimal. We
    keep them exact end-to-end by passing the strings through.
    trade_id (Binance field "t") rides along as the dedup key.

    produce() is NON-BLOCKING: it copies the record into librdkafka's internal
    queue and returns. A background thread does the actual sending. If that
    queue is full it raises BufferError — we drain it with poll() and retry.
    """
    payload = json.dumps({
        "symbol": symbol,
        "trade_id": trade_id,          # Binance "t" — dedup key downstream
        "price": price,                # raw string, e.g. "64123.45000000"
        "quantity": quantity,          # raw string
        "trade_time": trade_time_ms,   # epoch milliseconds, as Binance sends it
    })
    while True:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=symbol,
                value=payload,
                on_delivery=on_delivery,
            )
            return
        except BufferError:
            # Local queue full: let the background thread send + fire callbacks,
            # then retry the same record.
            log.warning("producer queue full; draining before retry")
            producer.poll(1.0)


async def connect_and_stream(producer: Producer, stop: asyncio.Event, state: dict) -> None:
    """One connection's lifetime: connect, read until it dies, return/raise.

    Sets state["received"] = True after the first trade arrives, so the caller
    can tell a working connection (reset backoff) from one that never delivered.

    KEY DETAIL — keepalive:
      We pass ping_interval=None to DISABLE the library's own keepalive pings.
      Why? Binance doesn't reliably reply to client-initiated pings, which would
      make the library think the link is dead and drop it every interval. We
      instead rely on two things:
        (a) the websockets library STILL auto-replies to Binance's *server*
            pings with pongs (that's independent of ping_interval) — which is
            exactly what Binance requires to keep us connected, and
        (b) RECV_TIMEOUT below: if no data arrives for a while, we conclude the
            socket is dead and bail out so the outer loop reconnects.
    """
    url = build_stream_url()
    log.info("connecting to %s", url)

    async with websockets.connect(url, ping_interval=None, close_timeout=5, max_size=2 ** 22) as ws:
        log.info("connected; streaming %s", ",".join(SYMBOLS))

        while not stop.is_set():
            # wait_for turns "no data for RECV_TIMEOUT" into a TimeoutError that
            # we re-raise as a connection failure (handled by the outer loop).
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
            except asyncio.TimeoutError as exc:
                raise ConnectionError(f"no messages for {RECV_TIMEOUT}s — assuming dead socket") from exc

            handle_message(producer, raw)
            state["received"] = True  # this connection is delivering data

            # Serve delivery callbacks for already-sent records without blocking.
            producer.poll(0)


def handle_message(producer: Producer, raw: str) -> None:
    """Parse one Binance frame and forward the trade to Kafka."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("non-JSON frame ignored: %r", raw[:120])
        return

    # Combined-stream frames wrap the payload under "data"; single streams don't.
    data = obj.get("data", obj)
    if data.get("e") != "trade":
        return  # ignore control frames / subscription acks

    try:
        publish(
            producer,
            symbol=data["s"],
            trade_id=int(data["t"]),       # Binance trade id (dedup key)
            price=str(data["p"]),          # keep exact string (Decimal later)
            quantity=str(data["q"]),
            trade_time_ms=int(data["T"]),
        )
    except (KeyError, ValueError) as exc:
        log.warning("malformed trade dropped: %s (%r)", exc, data)


async def run() -> None:
    """Outer supervisor loop: keep a connection alive forever, with backoff.

    Binance disconnects are NORMAL, not exceptional — a long-running market
    client must treat them as routine and just reconnect. The pattern:
      - on failure, sleep base*2**attempt (capped) + random jitter, then retry;
      - reset the backoff once a connection actually delivers data, so a brief
        blip doesn't permanently inflate our wait.
    Jitter avoids a 'thundering herd' if many clients reconnect at once.
    """
    producer = make_producer()
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    attempt = 0
    try:
        while not stop.is_set():
            state = {"received": False}
            try:
                await connect_and_stream(producer, stop, state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — any failure -> reconnect
                log.warning("connection lost: %s", exc)

            if state["received"]:
                attempt = 0  # last connection worked; start backoff fresh

            if stop.is_set():
                break

            delay = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempt)) + random.uniform(0, 1)
            attempt += 1
            log.info("reconnecting in %.1fs (attempt %d)", delay, attempt)
            await asyncio.sleep(delay)
    finally:
        # Graceful shutdown: flush the local buffer so in-flight records are
        # actually sent + acked BEFORE we exit. Without this, anything still
        # sitting in librdkafka's queue when the process dies is simply LOST —
        # the whole point of catching SIGTERM/SIGINT (see _install_signal_handlers)
        # is to reach this flush on a clean `docker compose stop`.
        # flush() blocks until the queue drains or the timeout elapses, serving
        # delivery callbacks meanwhile, and returns how many are STILL unsent.
        queued = len(producer)  # records still queued / in flight right now
        log.info("flushing producer before exit (%d queued)...", queued)
        remaining = producer.flush(15)
        if remaining:
            log.warning("flush timed out: %d message(s) NOT delivered", remaining)
        log.info("bye — delivered=%d failed=%d", STATS["delivered"], STATS["failed"])


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Stop cleanly on Ctrl-C / `docker stop` (SIGINT/SIGTERM).

    add_signal_handler isn't available on Windows event loops, so guard it —
    that way `python producer.py` still works for local poking on a dev box.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError):
            pass


if __name__ == "__main__":
    asyncio.run(run())
