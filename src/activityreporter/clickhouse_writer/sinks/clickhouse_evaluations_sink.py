from __future__ import annotations

from typing import ClassVar

from activityreporter.clickhouse_writer.sinks.clickhouse_sink_base import (
    ClickHouseSinkBase,
)
from activityreporter.clickhouse_writer.sinks.row_values import RowValues
from activityreporter.shared.models.evaluation import Evaluation
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class ClickHouseEvaluationsSink(ClickHouseSinkBase):
    table = "evaluations"
    columns: ClassVar[list[str]] = [
        "metric_id", "machine_id", "metric_name", "timestamp", "algorithm", "baseline", "lower", "upper", "is_anomaly",
    ]
    record_type = Evaluation

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.evaluations_topic

    def to_row(self, record: Evaluation) -> list:
        return [
            RowValues.metric_uuid(record.metric_id),
            record.machine_id,
            record.metric_name,
            RowValues.utc(record.timestamp),
            record.algorithm,
            float(record.baseline),
            None if record.lower is None else float(record.lower),
            float(record.upper),
            bool(record.is_anomaly),
        ]
