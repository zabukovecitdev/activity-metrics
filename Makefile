FLINK_PYTHON := flink/.venv/bin/python
KAFKA_CONNECTOR_JAR := flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar
KAFKA_CONNECTOR_URL := https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

.PHONY: agent collector metrics-writer anomaly-detector test up down

agent:
	uv run agent

collector:
	uv run collector

metrics-writer:
	uv run metrics-writer

anomaly-detector: $(FLINK_PYTHON) $(KAFKA_CONNECTOR_JAR)
	PYTHONPATH=src KAFKA_CONNECTOR_JAR=$(abspath $(KAFKA_CONNECTOR_JAR)) $(FLINK_PYTHON) -m activityreporter.anomaly_detector.main

$(FLINK_PYTHON):
	python3.11 -m venv flink/.venv
	flink/.venv/bin/pip install apache-flink==1.19.1

$(KAFKA_CONNECTOR_JAR):
	mkdir -p $(dir $(KAFKA_CONNECTOR_JAR))
	curl -sSL -o $(KAFKA_CONNECTOR_JAR) $(KAFKA_CONNECTOR_URL)

test:
	uv run pytest

# Kafka has no volume, so topics and offsets are reset; TimescaleDB data is kept.
up:
	docker compose down --remove-orphans
	docker compose up -d --build

down:
	docker compose down --remove-orphans
