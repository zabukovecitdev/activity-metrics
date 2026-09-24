FLINK_PYTHON := flink-jobs/.venv/bin/python
KAFKA_CONNECTOR_JAR := flink-jobs/lib/flink-sql-connector-kafka-3.2.0-1.19.jar
KAFKA_CONNECTOR_URL := https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

$(KAFKA_CONNECTOR_JAR):
	mkdir -p $(dir $(KAFKA_CONNECTOR_JAR))
	curl -sSL -o $(KAFKA_CONNECTOR_JAR) $(KAFKA_CONNECTOR_URL)

.PHONY: flink-example
flink-example: $(KAFKA_CONNECTOR_JAR)
	PYTHONPATH=src $(FLINK_PYTHON) flink-jobs/example_job.py

.PHONY: client collector metrics-writer test
client:
	uv run activityreporter

collector:
	uv run collector

metrics-writer:
	uv run metrics-writer

test:
	uv run pytest

# Starts the whole stack, or restarts it from scratch if it's already running.
# Kafka has no volume, so topics and offsets are reset; TimescaleDB data is kept.
.PHONY: run down
run:
	docker compose down --remove-orphans
	docker compose up -d --build

down:
	docker compose down --remove-orphans
