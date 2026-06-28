# Real-Time Crypto Analytics Pipeline

A working end-to-end stream: **Binance live trades → Kafka → consumer →
ClickHouse → Grafana**. The whole thing comes up with **one `docker compose up`**
on any machine — your dev box or the Ubuntu server — building everything from
source. No pre-built images, no machine-specific paths.

> **Status:** Phase 1 (working skeleton) complete. **Phase 2 Part A** migrated the
> event log from Redpanda to **real Apache Kafka in KRaft mode** (no ZooKeeper).
> Anomaly detection, exactly-once delivery, and benchmarking come in later phases.

---

## Architecture

```
                         ┌───────────────────────────────────────────────────────────┐
                         │                  docker compose (one host)                 │
                         │                                                             │
  Binance public WS      │   ┌──────────┐      ┌────────────┐      ┌──────────────┐   │
 (wss trade streams) ───────▶│ producer │─────▶│   Kafka    │─────▶│   consumer   │   │
   btcusdt@trade         │   │ (Python) │ pub  │  (KRaft)   │ sub  │  (Python)    │   │
   ethusdt@trade         │   └──────────┘      │  topic:    │      └──────┬───────┘   │
                         │                     │  "trades"  │     batched │ insert    │
                         │                     └─────┬──────┘     inserts  ▼           │
                         │                           │              ┌──────────────┐   │
                         │                    ┌──────▼──────┐       │  ClickHouse  │   │
                         │                    │  Kafka UI   │       │  crypto.     │   │
                         │                    │  :8080 (UI) │       │  trades      │   │
                         │                    └─────────────┘       └──────┬───────┘   │
                         │                                                 │ native    │
                         │                                          ┌──────▼───────┐   │
                         │                                          │   Grafana    │   │
                         │                                          │   :3000      │   │
                         │                                          └──────────────┘   │
                         └───────────────────────────────────────────────────────────┘

Data path:   producer → Kafka → consumer → ClickHouse → Grafana
Debug path:  Kafka UI (:8080) watches the topic; not part of the data path.

Kafka listeners:  containers → kafka:9092 (INTERNAL)   |   host → localhost:19092 (EXTERNAL)
```

### Components & key choices

| Service | Image / Build | Why |
|---|---|---|
| **kafka** | `apache/kafka` (KRaft) | Industry-standard event-streaming backbone; strongest connector ecosystem (Kafka Connect, Schema Registry). KRaft = no ZooKeeper. Official Apache image, single broker for single-node. |
| **kafka-init** | `apache/kafka` (one-shot) | Creates the `trades` topic with 3 partitions / RF 1, then exits. |
| **kafka-ui** | `provectuslabs/kafka-ui` | Web UI to *watch* topics, messages, and consumer-group lag — great for learning/debugging. |
| **clickhouse** | `clickhouse/clickhouse-server` | Columnar OLAP store; fast inserts + time-range scans. |
| **grafana** | `grafana/grafana` | Dashboards, datasource + dashboard provisioned **as code**. |
| **producer** | built from `./producer` | Binance WS → Kafka. |
| **consumer** | built from `./consumer` | Kafka → batched insert → ClickHouse. |

**Library choices (and why):**

- **`confluent-kafka`** (producer *and* consumer) — wraps librdkafka (C); the most
  maintained, fastest Python Kafka client. Its `produce()` is *non-blocking*
  (buffers locally, a background thread sends), so it fits the producer's async
  WebSocket loop without needing an async-specific Kafka library.
- **`websockets`** — mature async WS client; **auto-replies to Binance's server
  pings**, so we only write the reconnect logic, not heartbeat plumbing.
- **`clickhouse-connect`** (official, by ClickHouse Inc.) — clean column-oriented
  batch insert: `client.insert(table, rows, column_names=[...])`.

---

## Why Kafka, and how KRaft works

**Why real Apache Kafka** (Phase 2 replaced Redpanda with it): Kafka is the
industry-standard backbone for production event streaming, with by far the strongest
connector ecosystem — Kafka Connect, Schema Registry, and a huge catalog of
source/sink connectors. A real pipeline needs that integration surface. It's also what
enterprise/industrial shops (the deployment target) actually run in production. The
producer/consumer already spoke the Kafka protocol via `confluent-kafka`, so this was a
**migration, not a rewrite**: only broker addresses and the broker service changed.

**KRaft (Kafka Raft) — what replaced ZooKeeper.** Classic Kafka stored all cluster
metadata (topics, partitions, leaders, ACLs, configs) in a *separate* ZooKeeper
ensemble. KRaft moves that metadata into an internal Kafka log managed by **controller**
nodes using the Raft consensus protocol — so there's one fewer system to run, operate,
and fail. It's the modern, default architecture (ZooKeeper mode is removed in Kafka 4.x).

- **Controller quorum** = the set of controller nodes that vote to elect the leader of
  the metadata log (`KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093`). Here one node is the
  whole quorum. In a real multi-machine cluster you'd run 3 controllers so the metadata
  log survives losing one.
- **Combined mode**: our single node runs **both** roles — `controller` (owns metadata)
  and `broker` (stores partitions, serves traffic) — via
  `KAFKA_PROCESS_ROLES=broker,controller`. Fine for single-node; large clusters separate
  the roles.
- **Single broker is honest for one host.** Running 3 brokers on one machine gives **no
  real fault tolerance** — they share the same disk, kernel, and power. So we run one
  broker and set every replication factor to **1** (you can't replicate to brokers that
  don't exist).

**Advertised listeners — the #1 Kafka-in-Docker gotcha.** A client connects to a
*bootstrap* address, and Kafka replies with the **advertised** address to use for the
actual partition leader. If that advertised address is wrong, the first handshake
"works" but every produce/fetch afterwards fails. We expose two client listeners with
different advertised names so both audiences get a reachable address:

| Listener | Port | Advertised as | Who uses it |
|---|---|---|---|
| `INTERNAL` | 9092 | `kafka:9092` | other containers (producer, consumer, kafka-ui) |
| `EXTERNAL` | 19092 | `localhost:19092` | your host / dev box |
| `CONTROLLER` | 9093 | *(not advertised)* | internal Raft metadata traffic only |

---

## Project layout

```
.
├── docker-compose.yml          # the single orchestrator (7 services)
├── .env.example                # documented config template (copy to .env)
├── .gitignore
├── Makefile                    # up / down / logs / ps / topic / query / clean
├── producer/                   # Binance WS → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── consumer/                   # Kafka → ClickHouse (batched)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
├── clickhouse/
│   └── init/01_schema.sql       # auto-creates crypto.trades on first boot
└── grafana/
    └── provisioning/
        ├── datasources/clickhouse.yml
        └── dashboards/
            ├── provider.yml
            └── crypto-live.json
```

---

## Run locally

Prerequisites: **Docker** + **Docker Compose v2** (`docker compose`, not the old
`docker-compose`). First `up` needs internet (pulls images + installs the Grafana
ClickHouse plugin).

```bash
cp .env.example .env          # optional — sane defaults work without it
make up                       # = docker compose up -d --build
```

Without `make` (e.g. Windows dev box), run the compose commands directly:

```powershell
copy .env.example .env
docker compose up -d --build
```

Then open:

| URL | What |
|---|---|
| http://localhost:3000 | **Grafana** → dashboard **Crypto Live** (login `admin` / `admin`) |
| http://localhost:8080 | **Kafka UI** → cluster `crypto-local` → topic `trades`, watch messages |

Useful commands:

```bash
make ps               # service status / health
make logs             # tail everything
make logs-producer    # just the producer (watch connects/reconnects)
make logs-consumer    # just the consumer (watch "inserted N rows")
make query            # row count + latest price per symbol, straight from ClickHouse
make down             # stop (keeps data)
make clean            # stop AND wipe data volumes (full reset)
```

Manual ClickHouse peek:

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT symbol, count(), max(price) FROM crypto.trades GROUP BY symbol"
```

---

## Deploy on the Ubuntu server

The contract: **code on dev → push → pull on server → `docker compose up`**. Both
machines build from the same compose file, so there's nothing machine-specific.

```bash
# one-time
git clone <your-repo-url> crypto-pipeline && cd crypto-pipeline
cp .env.example .env            # edit if you want different symbols / passwords

# every deploy
git pull
make up                         # rebuilds changed images and restarts
```

`restart: unless-stopped` keeps producer/consumer alive across reboots. Named
volumes (`kafka_data`, `clickhouse_data`, `grafana_data`) persist data across
`make down`; use `make clean` only when you want a clean slate.

---

## Configuration (env vars)

Everything is read from `.env` (see `.env.example`). Compose falls back to the
defaults below if a var is unset, so the stack also boots with no `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `SYMBOLS` | `BTCUSDT,ETHUSDT` | Comma-separated Binance symbols to stream. |
| `BINANCE_WS_BASE` | `wss://stream.binance.com:9443` | Binance market-stream base URL. |
| `KAFKA_BROKER` | `kafka:9092` | Broker address used **inside** the network (INTERNAL listener; host uses `localhost:19092`). |
| `KAFKA_TOPIC` | `trades` | Topic the producer writes and consumer reads. |
| `KAFKA_GROUP_ID` | `clickhouse-writer` | Consumer group (offsets tracked per group). |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse hostname on the compose network. |
| `CLICKHOUSE_PORT` | `8123` | HTTP port the **consumer** uses (Grafana uses native `9000`). |
| `CLICKHOUSE_DB` | `crypto` | Database name. |
| `CLICKHOUSE_TABLE` | `trades` | Table name. |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / *(empty)* | Credentials. |
| `BATCH_SIZE` | `1000` | Flush to ClickHouse once this many rows are buffered… |
| `FLUSH_INTERVAL_SECONDS` | `2` | …or this many seconds pass — whichever first. |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | `admin` / `admin` | Grafana login. |
| `LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` for producer & consumer. |

> Changing `CLICKHOUSE_DB`/`CLICKHOUSE_TABLE` works automatically: the dashboard
> queries are unqualified (`FROM trades`) and resolve against the datasource's
> default database.

---

## How it works (the three ideas worth understanding)

### 1. Why the consumer batches inserts
ClickHouse's `MergeTree` writes every `INSERT` as an immutable **part** (a folder of
column files) and merges parts in the background. One insert per trade → thousands
of tiny parts → merge storms and the dreaded `TOO_MANY_PARTS` error. So the consumer
buffers rows and writes them in one shot, flushing on **`BATCH_SIZE` rows OR
`FLUSH_INTERVAL_SECONDS`**, whichever hits first. We use `consumer.consume(num_messages=BATCH_SIZE, timeout=FLUSH_INTERVAL)`
— it returns as soon as either limit is reached, so "batch by size or time" is free.
See [consumer/consumer.py](consumer/consumer.py).

### 2. Why these ClickHouse types
See [clickhouse/init/01_schema.sql](clickhouse/init/01_schema.sql).
- `symbol LowCardinality(String)` — only a few distinct values → dictionary-encoded, smaller & faster.
- `price/quantity Float64` — simple for a viz skeleton (Binance sends strings; we parse to float). Swap to `Decimal64(8)` later for exact money math.
- `trade_time DateTime64(3)` — Binance trade time is **epoch milliseconds**, so millisecond precision.
- `ingested_at … DEFAULT now64(3)` — set by ClickHouse on insert; later lets us measure end-to-end lag.
- `ORDER BY (symbol, trade_time)` — the sparse primary index, matching "price of symbol X over a time range".

### 3. How the WebSocket reconnect works
Binance **will** drop the socket (~24h connection cap, plus transient blips), so the
producer treats disconnects as routine. See [producer/producer.py](producer/producer.py):
- An outer supervisor loop reconnects forever with **exponential backoff + jitter**
  (`base·2^attempt`, capped, plus randomness to avoid thundering herds), resetting the
  backoff once a connection actually delivers data.
- Keepalive: we **disable client-side pings** (`ping_interval=None`) because Binance
  doesn't reliably pong them; the library still **auto-pongs Binance's server pings**
  (which is what keeps us connected), and a **receive timeout** detects a silently dead
  socket and forces a reconnect.

### Delivery semantics
**At-least-once.** The consumer disables auto-commit and commits Kafka offsets *only
after* a batch lands in ClickHouse. A crash mid-batch means those messages are re-read
on restart (a few possible duplicate rows — fine for Phase 1; exactly-once is a later
phase).

---

## Verify it's working

1. `make ps` → all services `running` / `healthy`.
2. **Producer:** `make logs-producer` shows `connected; streaming BTCUSDT,ETHUSDT`.
3. **Topic:** open Kafka UI (http://localhost:8080) → cluster `crypto-local` → topic `trades`
   → 3 partitions, live messages keyed by symbol (and check the consumer group's lag near 0).
4. **ClickHouse:** `make query` → row counts climbing, a realistic `last_price`.
5. **Grafana:** http://localhost:3000 → **Crypto Live** → price line moving, current-price stat updating.
6. **Reconnect:** `docker compose restart producer` → logs show backoff → reconnect → data resumes.

---

## Troubleshooting

- **Producer/consumer can't reach the broker (timeouts, "broker transport failure"):**
  almost always advertised listeners. Containers must use `kafka:9092` (INTERNAL); only
  the host uses `localhost:19092` (EXTERNAL). Check `docker compose logs kafka` and the
  `KAFKA_ADVERTISED_LISTENERS` value.
- **`kafka-init` failed / topic missing:** check `docker compose logs kafka-init`. It's a
  one-shot that creates `trades` (3 partitions, RF 1); rerun with `make topic` or
  `docker compose up kafka-init`.
- **Grafana datasource error / plugin missing:** the first `up` needs internet to
  install `grafana-clickhouse-datasource`. Check `docker compose logs grafana`.
- **Dashboard empty:** give it a minute (data must accumulate); confirm `make query`
  shows rows; ensure the dashboard time range covers "now".
- **No trades / producer reconnect loop:** verify outbound HTTPS/WSS to Binance is
  allowed from the host; some regions geo-block — try
  `BINANCE_WS_BASE=wss://data-stream.binance.vision` in `.env`.
- **Schema didn't apply:** init SQL runs only on a *fresh* ClickHouse volume. After
  changing it, `make clean` to wipe and re-init. (The consumer also creates the table
  idempotently on startup as a safety net.)
