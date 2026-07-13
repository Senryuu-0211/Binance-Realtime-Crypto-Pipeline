# Real-Time Crypto Analytics Pipeline

A working end-to-end stream: **Binance live trades → Kafka → consumer →
ClickHouse → Grafana**. The whole thing comes up with **one `docker compose up`**
on any machine — your dev box or the Ubuntu server — building everything from
source. No pre-built images, no machine-specific paths.

> **Status:** Phases 1–3.5 + 5–6 complete. Kafka KRaft · all-time peak gauge · rolling
> moving-average · **effectively-once** ingestion (`trade_id` dedup + `ReplacingMergeTree`,
> `Decimal64(8)`, hardened producer) · scheduled **health-check + Grafana alerting** ·
> **self-healing** trade-ID gap detection + REST backfill (no loss at the WebSocket edge) ·
> **schema governance** (Avro + Schema Registry, enforced BACKWARD compat) ·
> **benchmark** (**100k events/s, p99 613 ms** single-node — via consumer scaling + batch
> tuning). See the [Benchmark](#benchmark-single-node) section.

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
Encoding:    messages are Avro; producer registers the schema in Schema Registry
             (:8081), consumer fetches it back by id to decode. (Kafka UI reads
             the registry too, so it renders Avro messages + shows the schemas.)
Debug path:  Kafka UI (:8080) watches the topic; not part of the data path.

Kafka listeners:  containers → kafka:9092 (INTERNAL)   |   host → localhost:19092 (EXTERNAL)
```

### Components & key choices

| Service        | Image / Build                  | Why                                                                                                                                                                                     |
| -------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **kafka**      | `apache/kafka` (KRaft)         | Industry-standard event-streaming backbone; strongest connector ecosystem (Kafka Connect, Schema Registry). KRaft = no ZooKeeper. Official Apache image, single broker for single-node. |
| **kafka-init** | `apache/kafka` (one-shot)      | Creates the `trades` topic with 3 partitions / RF 1, then exits.                                                                                                                        |
| **schema-registry** | `confluentinc/cp-schema-registry` | Versioned message-schema **contract** store (Avro). Producers register the schema; consumers fetch it by id; the registry rejects breaking changes (BACKWARD compat). Works with the apache/kafka broker. |
| **kafka-ui**   | `provectuslabs/kafka-ui`       | Web UI to _watch_ topics, messages, and consumer-group lag — great for learning/debugging. Wired to the registry so it decodes Avro + shows schemas.                                    |
| **clickhouse** | `clickhouse/clickhouse-server` | Columnar OLAP store; fast inserts + time-range scans.                                                                                                                                   |
| **grafana**    | `grafana/grafana`              | Dashboards, datasource + dashboard provisioned **as code**.                                                                                                                             |
| **producer**   | built from `./producer`        | Binance WS → Kafka.                                                                                                                                                                     |
| **consumer**   | built from `./consumer`        | Kafka → batched insert → ClickHouse.                                                                                                                                                    |

**Library choices (and why):**

- **`confluent-kafka`** (producer _and_ consumer) — wraps librdkafka (C); the most
  maintained, fastest Python Kafka client. Its `produce()` is _non-blocking_
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
metadata (topics, partitions, leaders, ACLs, configs) in a _separate_ ZooKeeper
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
_bootstrap_ address, and Kafka replies with the **advertised** address to use for the
actual partition leader. If that advertised address is wrong, the first handshake
"works" but every produce/fetch afterwards fails. We expose two client listeners with
different advertised names so both audiences get a reachable address:

| Listener     | Port  | Advertised as      | Who uses it                                     |
| ------------ | ----- | ------------------ | ----------------------------------------------- |
| `INTERNAL`   | 9092  | `kafka:9092`       | other containers (producer, consumer, kafka-ui) |
| `EXTERNAL`   | 19092 | `localhost:19092`  | your host / dev box                             |
| `CONTROLLER` | 9093  | _(not advertised)_ | internal Raft metadata traffic only             |

---

## Peak tracking — "% of all-time high" (Phase 2 Part B)

A Grafana gauge per coin shows **current price ÷ all-time peak × 100**. The peak is
maintained cheaply and correctly:

- **`crypto.peak_prices`** — an `AggregatingMergeTree` table with one column
  `peak_state AggregateFunction(max, Float64)`. We store a max **state**, not a number.
  Why not `SELECT max(price) FROM trades` at query time? That full-scans the whole trades
  table on every refresh and slows down as data grows. `max` state is fixed-size and
  updated in O(1); reads touch only a few state rows, so cost stays flat. See
  [clickhouse/init/02_peak.sql](clickhouse/init/02_peak.sql).
- **`crypto.peak_prices_mv`** — a materialized view that maintains it. A ClickHouse MV is
  an **INSERT-time trigger**: it sees only the block being inserted into `trades`, never
  history. That's fine for `max` because the all-time max is monotonic
  (`max(all) = max(max(block₁), max(block₂), …)`).
- **Seeding (mandatory).** The MV only sees trades from when it started, so without
  seeding "all-time" would just mean "since the pipeline started". `make seed-peaks` runs
  [seeder/seed_peaks.py](seeder/seed_peaks.py): it fetches **monthly klines** per symbol
  from Binance REST (`/api/v3/klines?interval=1M&limit=1000`, taking each candle's `high`)
  and seeds the historical high. It's **idempotent** — re-running only ever moves the peak
  up (a `max` state can't be lowered), and it compacts with `OPTIMIZE … FINAL`.
- **⚠️ Reading it — the `maxMerge` trap.** A State column must be read with the `-Merge`
  combinator: `SELECT symbol, maxMerge(peak_state) FROM crypto.peak_prices GROUP BY symbol`.
  Reading `peak_state` directly returns raw state blobs (wrong numbers) **with no error** —
  it just silently lies. Always pair State columns with `maxMerge(...) + GROUP BY`.

> **Phase 4 note (not built yet):** a value >100% is _fleeting_ — the instant price
> exceeds the peak, the MV bumps the peak and the ratio falls back to ~100%, so the gauge
> never _sits_ above 100%. The future "new all-time high" alert must catch the **event**
> (price crossed the old peak) at the moment it happens, not poll the gauge state.

```bash
make seed-peaks   # seed/refresh all-time peaks (run once after first `up`, anytime to refresh)
make peaks        # read them back the correct way (maxMerge + GROUP BY)
```

---

## Moving average — trend line (Phase 2, final step)

The **Crypto Trend** dashboard ([grafana/.../moving-average.json](grafana/provisioning/dashboards/moving-average.json))
shows, per coin, the **price** line and a **moving-average** line on top of it — when price
is above its MA it's trending up vs its recent average, and vice versa.

**Why a direct query, not a materialized view (the key contrast with peak):** a rolling
average needs _history_ — to average the last N minutes you must look back over many rows.
A ClickHouse MV only ever sees the single block being inserted, so it **cannot** compute a
rolling window. Peak works as an MV because `max` is monotonic and per-block; a moving
average is not. So the MA is a **direct windowed query Grafana runs on each refresh** —
no MV, no seeding.

It's cheap because the day-`PARTITION` prunes the time range and it aggregates in two
cheap steps:

```sql
-- 1) bucket trades by minute (the "price" line), then
-- 2) a rolling average over the last $ma_window buckets (the smooth MA line)
SELECT time,
       avg(price) OVER (ORDER BY time ROWS BETWEEN $ma_window PRECEDING AND CURRENT ROW) AS moving_avg
FROM (
    SELECT toStartOfMinute(trade_time) AS time, avg(price) AS price
    FROM crypto.trades
    WHERE symbol = '$symbol' AND $__timeFilter(trade_time)
    GROUP BY time
)
ORDER BY time
```

- **`$ma_window`** is a Grafana variable (15 / 60 / 240 / 720 / 1440 minutes, default 60).
  Bigger window = more buckets averaged = **smoother** line. Switch 60 ↔ 1440 to see it.
- **Repeating panel** over the `$symbol` variable (one chart per coin) — chosen over a
  single 10-line multi-series chart because price+MA for 5 coins in one panel is unreadable;
  per-coin panels make each crossover obvious. Same `$symbol` pattern as the peak gauges.

---

## Pipeline health monitoring & alerting

Dashboards are _passive_ — they only help if someone is looking. A streaming pipeline needs
**active** monitoring that notices a stall on its own and alerts, before a stakeholder sees
stale numbers. (Same shape as watching an industrial sensor-telemetry stream for dropouts.)
The `healthcheck` service ([healthcheck/health_check.py](healthcheck/health_check.py)) runs
on a schedule and writes results to **`crypto.health_checks`**; Grafana reads that table to
both **show** health and **alert** on it.

**Three checks (each catches what the others miss):**

- **Freshness (overall):** `now() − max(trade_time)`. If the newest trade is older than
  `FRESHNESS_THRESHOLD_SECONDS` (default 120s) → `STALE` = the whole pipeline stopped.
- **Per-symbol stall:** last-trade age for _each_ symbol. If one symbol exceeds
  `SYMBOL_STALL_THRESHOLD_SECONDS` (default 300s) → `STALLED`. This catches a **partial**
  failure the overall check can't: overall `max()` is dominated by the busiest coin, so BTC
  flowing happily hides a silently-dead XRP subscription. Only the per-symbol view sees it.
- **Trade-ID gap:** Binance stamps each trade with a per-symbol monotonic id (`t`), so a jump
  `1000 → 1003` means ids 1001–1002 were **lost** — the WebSocket dropped for a moment and
  those trades happened during the gap. A `lagInFrame` window query counts the missing ids per
  symbol → `GAP_DETECTED`. Stall can't see this (the stream is flowing, just with holes); gap
  can't see a fully-dead stream (no "after" ids) — together they cover the whole spectrum of
  loss at the source edge.

**Design choices (the "why"):**

- **A 5-minute sleep-loop, not Airflow.** This is _one_ periodic job with no dependency
  graph. Airflow (scheduler + webserver + metadata DB + workers) earns its keep on multi-step
  DAGs with retries/backfills — over-engineering for a single recurring check. Reach for it
  when the dependency graph appears, not before.
- **Alerting in-stack via Grafana, no Slack/SMTP.** The check only records status rows;
  Grafana (already here) displays them and fires alert rules
  ([grafana/provisioning/alerting/health-alerts.yaml](grafana/provisioning/alerting/health-alerts.yaml)),
  keeping the stack self-contained. Production would attach a PagerDuty/Slack contact point —
  a notification-policy change, not a code change.
- **Loose per-symbol threshold** so a naturally-quiet low-volume coin doesn't false-alarm;
  tune all thresholds (and the interval) via env.
- **The monitor never crashes the pipeline:** a ClickHouse blip is logged and retried next
  cycle.
- **Who watches the watcher? (dead-man switch).** The three checks above use `noDataState: OK`,
  so if the healthcheck _service_ dies, no rows are written and those alerts would go silently OK —
  blinding us. A process can't reliably announce its own death (crash/OOM gives it no chance), so a
  fourth alert (**"Healthcheck monitor is DOWN"**) uses the opposite pattern: the healthcheck already
  writes a `freshness` row every cycle (its **heartbeat**), and this rule fires when that heartbeat
  goes **stale** (`now() − max(checked_at) > 660s`, i.e. > 2 cycles). After the service dies its old
  rows remain, so the age keeps climbing and the alert fires — unlike the no-data rules. (Residual
  SPOF: this detector is in-stack, so a whole-host outage — Grafana + ClickHouse down too — needs a
  heartbeat pushed to an _external_ dead-man service. That's an upgrade, not a replacement.)

See it on the **Pipeline Health** dashboard (freshness stat, per-symbol age table, lag-over-time,
plus trade-ID gaps), and the four rules in Grafana → Alerting. Thresholds: `HEALTH_CHECK_INTERVAL_SECONDS`,
`FRESHNESS_THRESHOLD_SECONDS`, `SYMBOL_STALL_THRESHOLD_SECONDS`, `GAP_CHECK_WINDOW_MINUTES`.

### Self-healing: from gap _detection_ to gap _remediation_

Detecting lost trades is only half the story. Kafka's `acks=all` + idempotence and ClickHouse's
dedup guarantee no loss **inside** the pipeline — but they can't recover an event we never received.
A WebSocket is fire-and-forget at the source: whatever traded during a disconnect is simply gone
from our stream. That was the last real no-data-loss hole.

The `backfiller` service ([backfiller/backfill.py](backfiller/backfill.py)) closes it. Every
`BACKFILL_INTERVAL_SECONDS` it scans the recent window for the exact gap **ranges** (not just
counts) and re-fetches the missing trades from Binance's REST history, making the pipeline
**self-healing**:

- **`/api/v3/historicalTrades`, not `/aggTrades`** — this is the subtle part. historicalTrades
  returns _raw_ trades whose `id` is the **same id space** as the WebSocket `t`, so a backfilled
  trade collapses onto the live one via our `(symbol, trade_id)` dedup key. aggTrades uses a
  _different_ aggregate id that would never dedup and would corrupt counts.
- **Idempotent by construction** — `trades` is a `ReplacingMergeTree(ingested_at)`, so re-inserting
  a trade we already have is a no-op after merge. Paging can overlap, cycles can repeat, runs can't
  double-count. That safety is what lets backfill be aggressive without fear.
- **Needs an API key** — historicalTrades is a `MARKET_DATA` endpoint (key in `X-MBX-APIKEY`, no
  request signature). Set `BINANCE_API_KEY` to enable it. With no key the service logs `disabled`
  and idles harmlessly — `docker compose up` still works and detection still runs; only auto-healing
  is off.
- **Audit trail** — each cycle writes a `backfill` row into `crypto.health_checks` (value = trades
  healed), so Grafana shows remediation right next to detection. `make gaps` prints both.

Manual one-shot pass (e.g. after a known outage): **`make backfill`** (runs with `BACKFILL_ONCE=1`).
Watch it live with `make logs-backfiller`. Tunables: `BACKFILL_INTERVAL_SECONDS`,
`BACKFILL_WINDOW_MINUTES`, `BACKFILL_MAX_GAP`.

---

## Schema governance (Avro + Schema Registry)

Without a registry, producer and consumer agree on the message shape **only by convention** —
nothing stops the producer from renaming a field or changing a type and silently breaking every
downstream reader. Messages are Avro, and the schema is an explicit, **versioned contract** held in
[Schema Registry](schemas/trade.avsc):

- **Wire format.** The producer registers the Avro schema under subject `trades-value`, the registry
  returns an **id**, and every message carries it (Confluent wire format: magic byte + 4-byte schema
  id + Avro payload). The consumer reads the id and fetches the **exact writer schema** from the
  registry — so it needs **no local schema copy** and always decodes with the schema the message was
  written with. (`make` targets and Kafka UI at :8080 → Schema Registry tab show the registered
  versions.)
- **Governance = enforced compatibility.** The registry is set to **BACKWARD** compatibility: a new
  schema version may add a field _with a default_ or remove one, but may **not** add a required field
  or change a type — a breaking change is **rejected at registration**, so it never reaches the topic.
  That is the "schema evolution stays safe" guarantee, and it's exactly the class of bug that silently
  corrupts a JSON pipeline.
- **Money stays exact.** `price`/`quantity` are Avro **`string`**, not float/double — the same raw
  decimal string Binance sends, parsed straight into ClickHouse `Decimal64(8)` at the sink. No float
  ever touches the value, registry or not.
- **Key unchanged.** Only the message _value_ is Avro; the key stays the plain `symbol` string, so
  partitioning and per-symbol ordering are identical to before.

The schema lives in git at [schemas/trade.avsc](schemas/trade.avsc) (canonical) and inline in the
producer/loadgen (self-contained images); if those ever diverge incompatibly, the registry rejects the
bad one rather than shipping it. The `backfiller` writes straight to ClickHouse (not through Kafka), so
it's unaffected by the encoding.

---

## Benchmark (single-node)

> ⚠️ The numbers below were measured with the **pre-Avro (JSON)** pipeline. Avro adds a small
> serialize/deserialize cost on both ends (fastavro is C-accelerated, so the delta should be modest);
> re-run `make loadtest` for exact post-Avro figures.

Binance's real feed is only tens of events/sec — far too low to find the pipeline's limits.
So `loadgen` ([loadgen/loadgen.py](loadgen/loadgen.py)) pushes **synthetic** trades (same
schema) into Kafka at a controllable rate, and we measure end-to-end.

```bash
docker compose stop producer             # measure clean synthetic load, not a mix
make loadtest RATE=10000 DURATION=60      # push 10k events/s for 60s (one generator)
make bench-latency                        # p50/p95/p99/max of (ingested_at − trade_time)
make bench-lag                            # consumer-group lag (growing = can't keep up)
docker compose start producer             # resume the real feed afterwards
```

A single generator process tops out near ~50k/s, so for higher rates run **two in parallel**
(distinct `LOADTEST_INSTANCE` so their `trade_id`s don't overlap and dedup away):

```bash
docker compose run --rm -e LOADTEST_INSTANCE=0 -e LOADTEST_RATE=50000 -e LOADTEST_DURATION=50 loadgen &
docker compose run --rm -e LOADTEST_INSTANCE=1 -e LOADTEST_RATE=50000 -e LOADTEST_DURATION=50 loadgen &
```

### Headline — **100,000 events/s end-to-end, p99 ≈ 613 ms, single node**

…reached by two compounding optimizations from a 1-consumer baseline. Measured on the dev
box (Docker engine: **16 vCPU, ~13.5 GB, WSL2**); single Kafka broker, topic `trades` 3
partitions, `ReplacingMergeTree` + `Decimal64(8)`, `FLUSH_INTERVAL=2s`.

**Baseline — 1 consumer, `BATCH_SIZE=1000`:**

| Push rate |   p50 |    p95 |    **p99** |     max | Verdict           |
| --------: | ----: | -----: | ---------: | ------: | ----------------- |
| 10,000 /s | 77 ms | 124 ms | **136 ms** | 2.6 s\* | healthy           |
| 30,000 /s | 46 ms |  96 ms | **168 ms** |  213 ms | healthy           |
| 50,000 /s | 3.7 s |  8.2 s |  **8.8 s** |  10.0 s | ceiling exceeded  |

<sub>\*the lone 2.6 s max at 10k was a first-batch/cold outlier; p99 is the honest figure.</sub>

**Optimization journey — two levers to 100k:**

| Config                              | Drain ceiling |  50k p99 |     100k p99 |
| ----------------------------------- | ------------: | -------: | -----------: |
| 1 consumer · batch 1000             |     ~47–50k/s |   8.8 s  |            — |
| **3 consumers** · batch 1000        |       ~80k/s  | **125 ms** |       15 s  |
| **3 consumers · batch 10000**       |   ~100–130k/s |        — | **613 ms** ✅ |

1. **Horizontal scaling** — `docker compose up -d --scale consumer=3`. All instances share
   the consumer group, so Kafka gives each one partition (≤1 per instance). Fixed the
   *parallelism* bottleneck: 50k went from 8.8 s → **125 ms**.
2. **Batch size 1000 → 10000** — 10× fewer, bigger ClickHouse inserts. Fixed the *per-insert
   overhead* bottleneck: 100k went from 15 s → **613 ms**, landing ~100–130k rows/s with
   consumer lag draining to 0.

This also makes the **latency/throughput trade-off** concrete: bigger batches buy throughput
by letting events wait a little longer to fill — p99 rose from 125 ms (batch 1000 @ 50k) to
613 ms (batch 10000 @ 100k), still comfortably sub-second.

### What breaks first, and the ceiling

The bottleneck is the **consumer's consume→insert throughput**, *not* ClickHouse: **zero
`TOO_MANY_PARTS`** even at 100k/s (batching held), and Kafka happily buffered any backlog.
So the scale path is horizontal. Two ceilings to know:

- **Partition cap.** We **key by symbol**, and `partition = hash(symbol) % N`. With **5
  symbols** at most 5 partitions ever carry data — so useful parallelism caps near **5
  consumers**. Going beyond needs more symbols, or a different partition key (which would
  cost per-symbol ordering).
- **Host CPU.** On this box the 2 load generators share 16 vCPU with 3 consumers +
  ClickHouse + Kafka, so the *measurement itself* competes for cores. A dedicated machine
  (the home server below) sees a higher ceiling.

### Why these numbers are honest

- **Latency clock starts at Kafka entry, not Binance.** `loadgen` stamps `trade_time` at
  publish, so `ingested_at − trade_time` excludes external network we don't control.
- **p99, not average.** The average hides the tail; p99 is what a production SLA promises —
  at 50k/s (1 consumer) the _average_ still looks okay while p99 had already blown out to seconds.
- **No dedup illusion.** unique `trade_id`s (per generator instance) mean `ReplacingMergeTree`
  doesn't collapse rows — we measure real insert work. (`LOADTEST_DUP_MODE=1` measures dedup
  overhead separately.)
- **Warmed up + context reported** (cores/RAM, batch size, partitions, consumer count).

> Honest-with-context beats impressive-but-vague: _"100k events/s, p99 613 ms, single node;
> reached by scaling consumers to the partition count + tuning batch size; bottleneck is the
> consumer insert path, parallelism capped by partition key."_

### Reproduce on the home server (Ubuntu 48 GB)

`make` is available on the server (unlike the Windows dev box), so the targets work directly.
With more RAM and dedicated cores, expect a higher ceiling than the dev-box numbers above.

```bash
git pull
# 100k profile: bigger batches + 3 consumers
echo "BATCH_SIZE=10000" >> .env                 # or edit .env
docker compose up -d --build                    # bring the stack up
docker compose up -d --scale consumer=3 consumer # 3 consumers (one per partition)

docker compose stop producer                    # clean benchmark
# two generators in parallel ≈ 100k/s for 50s:
docker compose run --rm -e LOADTEST_INSTANCE=0 -e LOADTEST_RATE=50000 -e LOADTEST_DURATION=50 loadgen &
docker compose run --rm -e LOADTEST_INSTANCE=1 -e LOADTEST_RATE=50000 -e LOADTEST_DURATION=50 loadgen &
wait
make bench-latency                              # p50/p95/p99 during/just after the run
make bench-lag                                  # confirm lag drains to 0
docker compose start producer                   # resume the real feed
```

> Tip: to make 3 consumers the permanent default (so a plain `docker compose up` starts
> three), add `deploy: { replicas: 3 }` to the `consumer` service instead of `--scale`. For
> the *true* ceiling, run `loadgen` from a **separate** machine so the generator doesn't
> steal cores from the pipeline it's measuring.

---

## Project layout

```
.
├── docker-compose.yml          # the single orchestrator (9 services + seeder/loadgen tools)
├── .env.example                # documented config template (copy to .env)
├── .gitignore
├── Makefile                    # up / logs / seed-peaks / dedup-check / backfill / loadtest / bench-* / clean …
├── producer/                   # Binance WS → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── consumer/                   # Kafka → ClickHouse (batched)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
├── seeder/                     # one-off: seed all-time peaks from Binance klines
│   ├── Dockerfile
│   ├── requirements.txt
│   └── seed_peaks.py
├── healthcheck/                # scheduled freshness + per-symbol stall + trade-ID gap monitor
│   ├── Dockerfile
│   ├── requirements.txt
│   └── health_check.py
├── backfiller/                 # self-healing: re-fetch WS-lost trades via historicalTrades
│   ├── Dockerfile
│   ├── requirements.txt
│   └── backfill.py
├── loadgen/                    # benchmark: synthetic load at a controllable rate
│   ├── Dockerfile
│   ├── requirements.txt
│   └── loadgen.py
├── schemas/
│   └── trade.avsc              # canonical Avro schema (the 'trades-value' contract)
├── clickhouse/
│   └── init/
│       ├── 01_schema.sql        # crypto.trades (ReplacingMergeTree, Decimal, trade_id)
│       ├── 02_peak.sql          # crypto.peak_prices (AggregatingMergeTree) + MV
│       └── 03_health.sql        # crypto.health_checks (MergeTree, TTL)
└── grafana/
    └── provisioning/
        ├── datasources/clickhouse.yml
        ├── alerting/health-alerts.yaml   # STALE / STALLED alert rules
        └── dashboards/
            ├── provider.yml
            ├── crypto-live.json       # live price per symbol
            ├── peak-tracking.json     # gauge: % of all-time peak
            ├── moving-average.json    # price vs rolling moving-average per symbol
            └── pipeline-health.json   # freshness + per-symbol stall + alerts
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

Then seed the all-time peaks once (needed for the peak gauge — see
[Peak tracking](#peak-tracking--of-all-time-high-phase-2-part-b)):

```bash
make seed-peaks       # fetch historical highs from Binance, populate crypto.peak_prices
```

Then open:

| URL                   | What                                                                                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| http://localhost:3000 | **Grafana** → **Crypto Live** · **Crypto Peak Tracking** · **Crypto Trend** · **Pipeline Health** (+ Alerting) — login `admin` / `admin` |
| http://localhost:8080 | **Kafka UI** → cluster `crypto-local` → topic `trades`, watch messages · **Schema Registry** tab shows registered schemas |
| http://localhost:8081 | **Schema Registry** REST API (e.g. `curl localhost:8081/subjects` → `["trades-value"]`)                                                   |

Useful commands:

```bash
make ps               # service status / health
make logs             # tail everything
make logs-producer    # just the producer (watch connects/reconnects)
make logs-consumer    # just the consumer (watch "inserted N rows")
make query            # row count + latest price per symbol, straight from ClickHouse
make seed-peaks       # seed/refresh all-time peaks from Binance klines (idempotent)
make peaks            # read all-time peak per symbol (maxMerge + GROUP BY)
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

| Variable                                        | Default                         | Meaning                                                                                      |
| ----------------------------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------- |
| `SYMBOLS`                                       | `BTCUSDT,ETHUSDT`               | Comma-separated Binance symbols to stream.                                                   |
| `BINANCE_WS_BASE`                               | `wss://stream.binance.com:9443` | Binance market-stream base URL.                                                              |
| `KAFKA_BROKER`                                  | `kafka:9092`                    | Broker address used **inside** the network (INTERNAL listener; host uses `localhost:19092`). |
| `KAFKA_TOPIC`                                   | `trades`                        | Topic the producer writes and consumer reads.                                                |
| `KAFKA_GROUP_ID`                                | `clickhouse-writer`             | Consumer group (offsets tracked per group).                                                  |
| `CLICKHOUSE_HOST`                               | `clickhouse`                    | ClickHouse hostname on the compose network.                                                  |
| `CLICKHOUSE_PORT`                               | `8123`                          | HTTP port the **consumer** uses (Grafana uses native `9000`).                                |
| `CLICKHOUSE_DB`                                 | `crypto`                        | Database name.                                                                               |
| `CLICKHOUSE_TABLE`                              | `trades`                        | Table name.                                                                                  |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`       | `default` / _(empty)_           | Credentials.                                                                                 |
| `BATCH_SIZE`                                    | `1000`                          | Flush to ClickHouse once this many rows are buffered…                                        |
| `FLUSH_INTERVAL_SECONDS`                        | `2`                             | …or this many seconds pass — whichever first.                                                |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | `admin` / `admin`               | Grafana login.                                                                               |
| `LOG_LEVEL`                                     | `INFO`                          | `DEBUG`/`INFO`/`WARNING`/`ERROR` for producer & consumer.                                    |

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
- `trade_id UInt64` — Binance's per-symbol trade id (field `t`); the **dedup key** (Phase 3).
- `price/quantity Decimal64(8)` — exact money to Binance's 8 dp (Phase 3, was Float64). Parsed from the raw string, never via float.
- `trade_time DateTime64(3)` — Binance trade time is **epoch milliseconds**, so millisecond precision.
- `ingested_at … DEFAULT now64(3)` — set by ClickHouse on insert; doubles as the **ReplacingMergeTree version**.
- `ENGINE = ReplacingMergeTree(ingested_at)`, `ORDER BY (symbol, trade_id)` — collapses duplicate `(symbol, trade_id)` rows at merge time. Day-`PARTITION` still prunes time-range scans.

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

### Delivery semantics — effectively-once via idempotent writes (Phase 3)

Kafka delivery stays **at-least-once**: the consumer disables auto-commit and commits
offsets _only after_ a batch lands in ClickHouse, so a crash mid-batch makes it re-read
and **re-insert** those messages. True distributed exactly-once is near-impossible, so
instead we make the **write idempotent** — reprocessing the same trades yields the same
final result (_effectively-once_):

- Each trade carries Binance's **`trade_id`** (field `t`), unique per symbol.
- `crypto.trades` is **`ReplacingMergeTree(ingested_at)`** with `ORDER BY (symbol, trade_id)`.
  Rows sharing `(symbol, trade_id)` collapse to one (highest `ingested_at`) at merge time.
- **⚠️ Dedup is at MERGE time (background), not on insert.** Until parts merge, duplicates
  coexist: `SELECT count()` may over-count; **`SELECT count() … FINAL`** (or a re-aggregating
  `GROUP BY`) is dedup-correct. Force it now with `OPTIMIZE TABLE crypto.trades FINAL`.
- **Money is `Decimal64(8)`**, not Float64 — exact to Binance's 8 dp. The producer forwards
  the raw price **string**, the consumer parses it straight into `Decimal` (no float between).
- **Peak still works:** the peak MV runs at _insert_ time (before dedup), so it sees a
  reprocessed trade twice — but `max(x, x) = x`, so duplicates can't change the peak. (This
  is safe for `max` only; `sum`/`count` would over-count and need `FINAL` input.)

**Producer side — no silent loss into Kafka** (`producer/producer.py`): `produce()` only
_queues_ locally (a background thread sends), so durability needs config, not luck:
`acks=all` (broker persists before acking), `enable.idempotence=true` (broker drops dups
from the producer's own retries), `delivery.timeout.ms` (bounded retry budget, then the
**delivery callback reports failure** — counted, never dropped silently), and a
**`flush()` on SIGTERM/SIGINT** so a clean `docker compose stop` drains in-flight records
before exit.

---

## Verify it's working

1. `make ps` → all services `running` / `healthy`.
2. **Producer:** `make logs-producer` shows `connected; streaming BTCUSDT,ETHUSDT`.
3. **Topic:** open Kafka UI (http://localhost:8080) → cluster `crypto-local` → topic `trades`
   → 3 partitions, live messages keyed by symbol (and check the consumer group's lag near 0).
4. **ClickHouse:** `make query` → row counts climbing, a realistic `last_price`.
5. **Peaks:** `make seed-peaks` then `make peaks` → sensible all-time highs for each symbol
   (e.g. BTC near its real ATH, not a tiny number).
6. **Grafana:** http://localhost:3000 → **Crypto Live** (price moving), **Crypto Peak
   Tracking** (gauge per coin), and **Crypto Trend** (price + moving-average line per coin;
   switch the `MA window` variable 60 ↔ 1440 to see the smoothing change).
7. **Health:** `docker compose logs healthcheck` shows an `OK` summary each cycle, and
   `crypto.health_checks` gains a new row set every `HEALTH_CHECK_INTERVAL_SECONDS`. The
   **Pipeline Health** dashboard shows freshness + per-symbol age. Stop the producer for a few
   minutes → freshness goes `STALE` and the Grafana alert fires; restart → it clears. Stop one
   symbol only → that symbol flags `STALLED` while the rest stay `OK`.
8. **Reconnect:** `docker compose restart producer` → logs show backoff → reconnect → data resumes.
9. **Self-healing backfill** (needs `BINANCE_API_KEY`): a producer restart usually leaves a small
   trade-ID gap → `make gaps` shows `GAP_DETECTED` with a missing count. Then `make backfill` (or wait
   for the `backfiller` loop) → run `make gaps` again: the `backfill` row shows the healed count and the
   next `trade_gap` check drops back to `OK`. `make logs-backfiller` shows `healing gap X-Y (N missing)`.
   With no API key the backfiller logs `disabled` and idles — detection still works.

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
- **Schema didn't apply:** init SQL runs only on a _fresh_ ClickHouse volume. After
  changing it, `make clean` to wipe and re-init. (The consumer also creates the table
  idempotently on startup as a safety net.)
- **Peak gauge empty / `peak_prices` doesn't exist:** on an already-running ClickHouse the
  init `02_peak.sql` won't re-run — `make seed-peaks` applies the peak schema (table + MV)
  _and_ seeds it. Run it once after `up`.
- **Seeded peaks look tiny / wrong:** the kline `high` is index 2 of each array; a tiny
  number means it's being read from the wrong field. If you get **HTTP 451**, Binance is
  geo-blocking — set `BINANCE_REST_BASE=https://data-api.binance.vision` in `.env`.
