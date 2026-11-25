from __future__ import annotations

from activity_metrics.clickhouse_writer.sinks.row_values import RowValues
from activity_metrics.clickhouse_writer.sinks.clickhouse_sink_base import ClickHouseSinkBase
from activity_metrics.shared.models.metric import Metric
from activity_metrics.shared.settings.kafka_settings import KafkaSettings


class ClickHouseMetricsSink(ClickHouseSinkBase):
    table = "metrics"
    columns = ["metric_id", "machine_id", "name", "timestamp", "type", "unit", "value", "attributes"]
    record_type = Metric

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.raw_metrics_topic

    def to_row(self, record: Metric) -> list:
        return [
            RowValues.metric_uuid(record.metric_id),
            record.machine_id,
            record.name,
            RowValues.utc(record.timestamp),
            record.type,
            record.unit,
            record.value,
            record.attributes or {},
        ]
