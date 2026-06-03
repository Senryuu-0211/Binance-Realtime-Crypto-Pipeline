# Real-Time Crypto Analytics Pipeline — Phase 1

A working end-to-end stream: **Binance live trades → Redpanda → consumer →
ClickHouse → Grafana**. The whole thing comes up with **one `docker compose up`**
on any machine — your dev box or the Ubuntu server — building everything from
source. No pre-built images, no machine-specific paths.

> **Phase 1 scope:** get the bytes flowing and visualized. Anomaly detection,
> exactly-once delivery, and benchmarking are deliberately **out of scope** —
> they come in later phases.

---

## Architecture

```
                         ┌───────────────────────────────────────────────────────────┐
                         │                  docker compose (one host)                 │
                         │                                                             │
  Binance public WS      │   ┌──────────┐      ┌────────────┐      ┌──────────────┐   │
 (wss trade streams) ───────▶│ producer │─────▶│  Redpanda  │─────▶│   consumer   │   │
   btcusdt@trade         │   │ (Python) │ pub  │  topic:    │ sub  │  (Python)    │   │
   ethusdt@trade         │   └──────────┘      │  "trades"  │      └──────┬───────┘   │
                         │                     └─────┬──────┘   batched   │ insert    │
                         │                           │          inserts   ▼           │
                         │                    ┌──────▼──────┐      ┌──────────────┐   │
                         │                    │  Redpanda   │      │  ClickHouse  │   │
                         │                    │  Console    │      │  crypto.     │   │
                         │                    │  :8080 (UI) │      │  trades      │   │
                         │                    └─────────────┘      └──────┬───────┘   │
                         │                                                │ native    │
                         │                                         ┌──────▼───────┐   │
                         │                                         │   Grafana    │   │
                         │                                         │   :3000      │   │
                         │                                         └──────────────┘   │
                         └───────────────────────────────────────────────────────────┘

Data path:   producer → Redpanda → consumer → ClickHouse → Grafana
Debug path:  Redpanda Console (:8080) watches the topic; not part of the data path.
```

### Components & key choices

| Service | Image / Build | Why |
|---|---|---|
| **redpanda** | `redpandadata/redpanda` | Kafka-compatible event log, single binary (no ZooKeeper/JVM). One broker is plenty for single-node. |
| **redpanda-console** | `redpandadata/console` | Web UI to *watch* messages flow — great for learning/debugging. |
| **clickhouse** | `clickhouse/clickhouse-server` | Columnar OLAP store; fast inserts + time-range scans. |
| **grafana** | `grafana/grafana` | Dashboards, datasource + dashboard provisioned **as code**. |
| **producer** | built from `./producer` | Binance WS → Redpanda. |
| **consumer** | built from `./consumer` | Redpanda → batched insert → ClickHouse. |

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

## Project layout

```
.
├── docker-compose.yml          # the single orchestrator (6 services)
├── .env.example                # documented config template (copy to .env)
├── .gitignore
├── Makefile                    # up / down / logs / ps / query / clean
├── producer/                   # Binance WS → Redpanda
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── consumer/                   # Redpanda → ClickHouse (batched)
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
| http://localhost:8080 | **Redpanda Console** → topic `trades`, watch messages |

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
volumes (`redpanda_data`, `clickhouse_data`, `grafana_data`) persist data across
`make down`; use `make clean` only when you want a clean slate.

---

## Configuration (env vars)

Everything is read from `.env` (see `.env.example`). Compose falls back to the
defaults below if a var is unset, so the stack also boots with no `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `SYMBOLS` | `BTCUSDT,ETHUSDT` | Comma-separated Binance symbols to stream. |
| `BINANCE_WS_BASE` | `wss://stream.binance.com:9443` | Binance market-stream base URL. |
| `KAFKA_BROKER` | `redpanda:9092` | Broker address used **inside** the network (host: `localhost:19092`). |
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
3. **Topic:** open Redpanda Console (http://localhost:8080) → `trades` → live messages keyed by symbol.
4. **ClickHouse:** `make query` → row counts climbing, a realistic `last_price`.
5. **Grafana:** http://localhost:3000 → **Crypto Live** → price line moving, current-price stat updating.
6. **Reconnect:** `docker compose restart producer` → logs show backoff → reconnect → data resumes.

---

## Troubleshooting

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
