# =============================================================================
# Convenience shortcuts around `docker compose`.
# On the Ubuntu server:  make up   |   make logs   |   make down
# On a Windows dev box without `make`, just run the docker compose commands
# shown in each recipe directly (see README "Run locally").
# =============================================================================

# Use the v2 plugin syntax (`docker compose`, not the old `docker-compose`).
COMPOSE := docker compose

# Benchmark knobs (override on the CLI, e.g. `make loadtest RATE=30000 DURATION=45`).
RATE ?= 10000
DURATION ?= 60

.PHONY: help up down restart logs logs-producer logs-consumer ps build topic query seed-peaks peaks dedup-check loadtest bench-latency bench-throughput bench-lag clean

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up:              ## Build images and start the whole stack in the background
	$(COMPOSE) up -d --build

down:            ## Stop and remove containers (KEEPS data volumes)
	$(COMPOSE) down

restart:         ## Recreate containers (after code/config changes)
	$(COMPOSE) up -d --build

logs:            ## Tail logs from all services
	$(COMPOSE) logs -f

logs-producer:   ## Tail only the producer
	$(COMPOSE) logs -f producer

logs-consumer:   ## Tail only the consumer
	$(COMPOSE) logs -f consumer

ps:              ## Show service status / health
	$(COMPOSE) ps

build:           ## Rebuild producer/consumer images without starting
	$(COMPOSE) build

topic:           ## Describe the trades topic (partitions, replicas, offsets)
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --describe --topic $${KAFKA_TOPIC:-trades}

query:           ## Quick sanity check: row counts + latest price per symbol
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT symbol, count() AS rows, max(price) AS last_price FROM crypto.trades GROUP BY symbol ORDER BY symbol"

seed-peaks:      ## Seed all-time peak prices from Binance klines (idempotent, on demand)
	$(COMPOSE) run --rm --build seeder

peaks:           ## Show seeded/maintained all-time peak per symbol (correct maxMerge read)
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT symbol, maxMerge(peak_state) AS all_time_peak FROM crypto.peak_prices GROUP BY symbol ORDER BY symbol"

dedup-check:     ## Raw count (may include dups) vs FINAL count (dedup'd) per symbol
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT symbol, count() AS raw_rows, uniqExact(trade_id) AS unique_trades FROM crypto.trades GROUP BY symbol ORDER BY symbol"
	@echo "-- after dedup (FINAL): --"
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT symbol, count() AS final_rows FROM crypto.trades FINAL GROUP BY symbol ORDER BY symbol"

loadtest:        ## Push synthetic load: make loadtest RATE=10000 DURATION=60 (stop the real producer first!)
	$(COMPOSE) run --rm --build -e LOADTEST_RATE=$(RATE) -e LOADTEST_DURATION=$(DURATION) loadgen

bench-latency:   ## End-to-end latency percentiles (ms) over the last 2 minutes
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT count() AS events, \
		        round(quantile(0.50)(lat),1) AS p50_ms, \
		        round(quantile(0.95)(lat),1) AS p95_ms, \
		        round(quantile(0.99)(lat),1) AS p99_ms, \
		        max(lat) AS max_ms \
		 FROM (SELECT toUnixTimestamp64Milli(ingested_at) - toUnixTimestamp64Milli(trade_time) AS lat \
		       FROM crypto.trades WHERE ingested_at > now() - INTERVAL 2 MINUTE) \
		 FORMAT Vertical"

bench-throughput: ## Rows landed per second over the last minute (sustained ingest rate)
	$(COMPOSE) exec clickhouse clickhouse-client --query \
		"SELECT count() AS rows_1min, round(count() / 60, 0) AS rows_per_sec \
		 FROM crypto.trades WHERE ingested_at > now() - INTERVAL 1 MINUTE"

bench-lag:       ## Consumer-group lag (growing unbounded = pipeline can't keep up)
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server kafka:9092 --describe --group $${KAFKA_GROUP_ID:-clickhouse-writer}

clean:           ## Stop everything AND delete data volumes (full reset)
	$(COMPOSE) down -v
