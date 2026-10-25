from __future__ import annotations

from datetime import timezone

from activityreporter.clickhouse_writer.sinks.row_values import RowValues
from activityreporter.clickhouse_writer.sinks.clickhouse_sink_base import ClickHouseSinkBase
from activityreporter.shared.models.machine import Machine
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class ClickHouseMachinesSink(ClickHouseSinkBase):
    table = "machines"
    columns = [
        "machine_id", "hostname", "os", "os_version", "architecture", "cores", "total_memory", "total_disk",
        "last_boot", "last_seen_at",
    ]
    record_type = Machine

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.machines_topic

    def to_row(self, record: Machine) -> list:
        return [
            record.machine_id, record.hostname, record.os, record.os_version, record.architecture, record.cores,
            record.total_memory, record.total_disk,
            record.last_boot.astimezone(timezone.utc), RowValues.utc(record.last_seen_at),
        ]
