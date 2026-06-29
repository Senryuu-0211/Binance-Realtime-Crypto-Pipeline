# =============================================================================
# Convenience shortcuts around `docker compose`.
# On the Ubuntu server:  make up   |   make logs   |   make down
# On a Windows dev box without `make`, just run the docker compose commands
# shown in each recipe directly (see README "Run locally").
# =============================================================================

# Use the v2 plugin syntax (`docker compose`, not the old `docker-compose`).
COMPOSE := docker compose

.PHONY: help up down restart logs logs-producer logs-consumer ps build topic query seed-peaks peaks clean

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

clean:           ## Stop everything AND delete data volumes (full reset)
	$(COMPOSE) down -v
