from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from activityreporter.clickhouse_writer.service import TopicWriter
from activityreporter.shared.models import Evaluation, Machine, Metric
from activityreporter.shared.settings import KafkaSettings


def utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc)


def metric_uuid(metric_id: Any) -> uuid.UUID:
    # str() turns None or a number into an invalid hex string, so every bad id raises ValueError.
    return uuid.UUID(str(metric_id))


def iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"{value!r} has no UTC offset")
    return parsed.astimezone(timezone.utc)


def non_empty(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"expected a non-empty string, got {value!r}")
    return value


class MetricsWriter(TopicWriter):
    table = "metrics"
    columns = ["metric_id", "machine_id", "name", "timestamp", "type", "unit", "value", "attributes"]
    record_type = Metric

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.raw_metrics_topic

    def to_row(self, m: Metric) -> list:
        return [
            metric_uuid(m.metric_id), m.machine_id, m.name, utc(m.timestamp), m.type, m.unit, m.value, m.attributes or {},
        ]


class EvaluationsWriter(TopicWriter):
    table = "evaluations"
    columns = [
        "metric_id", "machine_id", "metric_name", "timestamp", "algorithm", "baseline", "lower", "upper", "is_anomaly",
    ]
    record_type = Evaluation

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.evaluations_topic

    def to_row(self, e: Evaluation) -> list:
        return [
            metric_uuid(e.metric_id), non_empty(e.machine_id), non_empty(e.metric_name), utc(e.timestamp),
            non_empty(e.algorithm), float(e.baseline), None if e.lower is None else float(e.lower), float(e.upper),
            bool(e.is_anomaly),
        ]


class MachinesWriter(TopicWriter):
    table = "machines"
    columns = [
        "machine_id", "hostname", "os", "os_version", "architecture", "cores", "total_memory", "total_disk",
        "last_boot", "observed_at",
    ]
    record_type = Machine

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.machines_topic

    def to_row(self, m: Machine) -> list:
        return [
            non_empty(m.machine_id), m.hostname, m.os, m.os_version, m.architecture, int(m.cores), int(m.total_memory),
            int(m.total_disk), iso_utc(m.last_boot), utc(m.observed_at),
        ]


WRITERS: list[type[TopicWriter]] = [MetricsWriter, EvaluationsWriter, MachinesWriter]
