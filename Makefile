FLINK_PYTHON := flink/.venv/bin/python
KAFKA_CONNECTOR_JAR := flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar
KAFKA_CONNECTOR_URL := https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

.PHONY: agent collector clickhouse-writer anomaly-detector smoke test up down migrate migrate-down migrate-new

agent:
	uv run agent

collector:
	uv run collector

clickhouse-writer:
	uv run clickhouse-writer

anomaly-detector: $(FLINK_PYTHON) $(KAFKA_CONNECTOR_JAR)
	PYTHONPATH=src KAFKA_CONNECTOR_JAR=$(abspath $(KAFKA_CONNECTOR_JAR)) $(FLINK_PYTHON) -m activityreporter.anomaly_detector.main

$(FLINK_PYTHON):
	python3.11 -m venv flink/.venv
	flink/.venv/bin/pip install apache-flink==1.19.1

$(KAFKA_CONNECTOR_JAR):
	mkdir -p $(dir $(KAFKA_CONNECTOR_JAR))
	curl -sSL -o $(KAFKA_CONNECTOR_JAR) $(KAFKA_CONNECTOR_URL)

# Checks a running stack end to end; WAIT=300 make smoke retries for up to 5 minutes.
smoke:
	scripts/smoke-test.sh

test:
	uv run pytest

# Kafka has no volume, so topics and offsets are reset; ClickHouse data is kept.
up:
	docker compose down --remove-orphans
	docker compose up -d --build

down:
	docker compose down --remove-orphans

migrate:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down 1

# make migrate-new name=add_foo_to_metrics
migrate-new:
	docker compose run --rm --no-deps --user $$(id -u):$$(id -g) migrate create -ext sql -dir /migrations -seq $(name)
