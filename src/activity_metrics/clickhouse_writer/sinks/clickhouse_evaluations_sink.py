from __future__ import annotations

from typing import ClassVar

from activity_metrics.clickhouse_writer.sinks.clickhouse_sink_base import (
    ClickHouseSinkBase,
)
from activity_metrics.clickhouse_writer.sinks.row_values import RowValues
from activity_metrics.shared.models.evaluation import Evaluation
from activity_metrics.shared.settings.kafka_settings import KafkaSettings


class ClickHouseEvaluationsSink(ClickHouseSinkBase):
    table = "evaluations"
    columns: ClassVar[list[str]] = [
        "metric_id",
        "machine_id",
        "metric_name",
        "timestamp",
        "algorithm",
        "baseline",
        "lower",
        "upper",
        "is_anomaly",
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
