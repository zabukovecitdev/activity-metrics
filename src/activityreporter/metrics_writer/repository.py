from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from kafka import KafkaConsumer

from activityreporter.shared.metrics import ProcessedMetric

logger = logging.getLogger(__name__)

POLL_TIMEOUT_MS = 1000

METRICS_TABLE = "metrics"
METRICS_COLUMNS = ["machine_id", "name", "timestamp", "type", "unit", "value", "attributes", "is_anomaly"]


class ClickHouseMetricsRepository:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
    ):
        self._connection = dict(host=host, port=port, username=username, password=password, database=database)
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._client: Client | None = None

    @classmethod
    def from_env(cls) -> ClickHouseMetricsRepository:
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "user"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "password"),
            database=os.environ.get("CLICKHOUSE_DB", "metrics"),
        )

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(**self._connection)

    def insert_batch(self, metrics_batch: list[ProcessedMetric]) -> None:
        if not metrics_batch:
            return

        rows = [to_row(m) for m in metrics_batch]

        for attempt in range(1, self._max_retries + 1):
            try:
                self._client.insert(METRICS_TABLE, rows, column_names=METRICS_COLUMNS)
                return
            except ClickHouseError:
                logger.exception(
                    "Failed to write metrics batch to ClickHouse (attempt %d/%d)",
                    attempt,
                    self._max_retries,
                )
                if attempt == self._max_retries:
                    raise
                time.sleep(self._backoff_base_seconds * (2 ** (attempt - 1)))

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> ClickHouseMetricsRepository:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def to_row(metric: ProcessedMetric) -> list:
    return [
        metric.machine_id,
        metric.name,
        datetime.fromtimestamp(metric.timestamp, timezone.utc),
        metric.type,
        metric.unit,
        metric.value,
        metric.attributes or {},
        metric.is_anomaly,
    ]


class KafkaProcessedMetricsRepository:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    @classmethod
    def from_env(cls) -> KafkaProcessedMetricsRepository:
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            topic=os.environ.get("KAFKA_PROCESSED_METRICS_TOPIC", "processed_metrics"),
            group_id=os.environ.get("KAFKA_CONSUMER_GROUP_ID", "clickhouse-writer"),
        )

    def poll(self, max_records: int) -> list[ProcessedMetric]:
        polled = self._consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=max_records)
        return [m for records in polled.values() for m in map(parse_record, records) if m is not None]

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> KafkaProcessedMetricsRepository:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def parse_record(record) -> ProcessedMetric | None:
    try:
        return ProcessedMetric(**json.loads(record.value))
    except (TypeError, ValueError) as e:
        logger.error(
            "Skipping malformed record %s[%s]@%s: %r", record.topic, record.partition, record.offset, e
        )
        return None
