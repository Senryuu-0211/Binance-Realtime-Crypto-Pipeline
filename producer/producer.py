"""
Producer: Binance live trades  ->  Redpanda topic.

What it does, end to end:
  1. Opens a WebSocket to Binance's public trade stream for the configured symbols.
  2. For every trade tick, publishes a small JSON event to the Redpanda topic,
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
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:9092")
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


def on_delivery(err, msg):
    """Called (by producer.poll) once the broker acknowledges — or rejects — a
    message. produce() returns immediately; THIS is where we learn the outcome.
    """
    if err is not None:
        log.error("delivery FAILED: %s", err)


def make_producer() -> Producer:
    """confluent-kafka Producer config.

    Note these are *client-side* knobs; produce() never blocks on the network.
    """
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "binance-producer",
        # Wait up to 50ms to batch messages together -> fewer, bigger requests.
        "linger.ms": 50,
        # Cheap CPU for less network: compress batches.
        "compression.type": "lz4",
        # Durability: wait for the broker to persist before acking.
        "acks": "all",
    })


def publish(producer: Producer, symbol: str, price: float, quantity: float, trade_time_ms: int) -> None:
    """Queue one trade event. Keyed by symbol so all of a symbol's trades land
    on the same partition (preserves per-symbol ordering).

    produce() is NON-BLOCKING: it copies the record into librdkafka's internal
    queue and returns. A background thread does the actual sending. If that
    queue is full it raises BufferError — we drain it with poll() and retry.
    """
    payload = json.dumps({
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
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
    """Parse one Binance frame and forward the trade to Redpanda."""
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
            price=float(data["p"]),
            quantity=float(data["q"]),
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
        # Block until every queued message is delivered (or times out).
        log.info("flushing producer before exit...")
        producer.flush(10)
        log.info("bye")


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
