from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from kafka import KafkaConsumer

from activityreporter.shared.evaluations import Evaluation
from activityreporter.shared.machines import Machine
from activityreporter.shared.metrics import Metric

logger = logging.getLogger(__name__)

POLL_TIMEOUT_MS = 1000


DEFAULT_GROUP_ID = "clickhouse-writer"


@dataclass(frozen=True)
class Sink:
    """One Kafka topic written to one ClickHouse table.

    The topic is read from the environment variable `topic_env`, the same one
    the producer of that topic uses, and falls back to `default_topic`.
    """
    table: str
    columns: list[str]
    record_type: type
    to_row: Callable[[Any], list]
    topic_env: str
    default_topic: str

    def topic(self) -> str:
        return os.environ.get(self.topic_env, self.default_topic)


def utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc)


def metric_uuid(metric_id: Any) -> uuid.UUID:
    # str() turns None or a number into an invalid hex string, so every bad id raises ValueError.
    return uuid.UUID(str(metric_id))


METRICS = Sink(
    table="metrics",
    columns=["metric_id", "machine_id", "name", "timestamp", "type", "unit", "value", "attributes"],
    record_type=Metric,
    to_row=lambda m: [
        metric_uuid(m.metric_id), m.machine_id, m.name, utc(m.timestamp), m.type, m.unit, m.value, m.attributes or {},
    ],
    topic_env="KAFKA_RAW_METRICS_TOPIC",
    default_topic="raw_metrics",
)


def iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"{value!r} has no UTC offset")
    return parsed.astimezone(timezone.utc)


def non_empty(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"expected a non-empty string, got {value!r}")
    return value


EVALUATIONS = Sink(
    table="evaluations",
    columns=[
        "schema_version", "metric_id", "machine_id", "metric_name", "timestamp", "value", "algorithm",
        "algorithm_version", "params", "baseline", "lower", "upper", "score", "threshold", "is_anomaly",
        "direction", "details", "detected_at",
    ],
    record_type=Evaluation,
    to_row=lambda e: [
        e.schema_version, metric_uuid(e.metric_id), e.machine_id, e.metric_name, utc(e.timestamp), e.value,
        e.algorithm, e.algorithm_version, e.params or {}, e.baseline, e.lower, e.upper, e.score, e.threshold,
        bool(e.is_anomaly), e.direction, e.details or {}, utc(e.detected_at),
    ],
    topic_env="KAFKA_EVALUATIONS_TOPIC",
    default_topic="evaluations",
)

MACHINES = Sink(
    table="machines",
    columns=[
        "machine_id", "hostname", "os", "os_version", "architecture", "cores", "total_memory", "total_disk",
        "last_boot", "observed_at",
    ],
    record_type=Machine,
    to_row=lambda m: [
        non_empty(m.machine_id), m.hostname, m.os, m.os_version, m.architecture, int(m.cores), int(m.total_memory),
        int(m.total_disk), iso_utc(m.last_boot), utc(m.observed_at),
    ],
    topic_env="KAFKA_MACHINES_TOPIC",
    default_topic="machines",
)

SINKS = [METRICS, EVALUATIONS, MACHINES]


class ClickHouseRepository:
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
    def from_env(cls) -> ClickHouseRepository:
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "user"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "password"),
            database=os.environ.get("CLICKHOUSE_DB", "metrics"),
        )

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(**self._connection)

    def insert_batch(self, sink: Sink, rows: list[list]) -> None:
        if not rows:
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                self._client.insert(sink.table, rows, column_names=sink.columns)
                return
            except ClickHouseError:
                logger.exception(
                    "Failed to write %s batch to ClickHouse (attempt %d/%d)",
                    sink.table,
                    attempt,
                    self._max_retries,
                )
                if attempt == self._max_retries:
                    raise
                time.sleep(self._backoff_base_seconds * (2 ** (attempt - 1)))

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> ClickHouseRepository:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class KafkaRecordsRepository:
    """One consumer for every sink's topic; each record is converted with its topic's sink."""

    def __init__(self, sinks_by_topic: dict[str, Sink], bootstrap_servers: str, group_id: str):
        self._sinks_by_topic = sinks_by_topic
        self._consumer = KafkaConsumer(
            *sinks_by_topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    @classmethod
    def from_env(cls, sinks: list[Sink]) -> KafkaRecordsRepository:
        return cls(
            {sink.topic(): sink for sink in sinks},
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            group_id=os.environ.get("KAFKA_CONSUMER_GROUP_ID", DEFAULT_GROUP_ID),
        )

    def poll(self, max_records: int) -> dict[str, list[list]]:
        """ClickHouse rows for the polled records by table; malformed records are logged and dropped."""
        polled = self._consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=max_records)
        rows: dict[str, list[list]] = {}
        for partition, records in polled.items():
            sink = self._sinks_by_topic[partition.topic]
            parsed = [row for row in (parse_record(sink, record) for record in records) if row is not None]
            if parsed:
                rows.setdefault(sink.table, []).extend(parsed)
        return rows

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> KafkaRecordsRepository:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def from_json(record_type: type, data: dict[str, Any]):
    """`record_type` built from `data`, ignoring keys it doesn't know.

    A producer may add fields before every consumer is deployed with them.
    """
    if not isinstance(data, dict):
        raise TypeError(f"expected a JSON object, got {type(data).__name__}")
    known = {f.name for f in fields(record_type)}
    return record_type(**{k: v for k, v in data.items() if k in known})


def parse_record(sink: Sink, record) -> list | None:
    # Converting here, not at insert time, keeps a bad record from failing every retry of its batch.
    try:
        return sink.to_row(from_json(sink.record_type, json.loads(record.value)))
    except (TypeError, ValueError) as e:
        logger.error(
            "Skipping malformed record %s[%s]@%s: %r", record.topic, record.partition, record.offset, e
        )
        return None
