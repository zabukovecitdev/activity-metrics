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
        "last_boot", "observed_at",
    ]
    record_type = Machine

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.machines_topic

    def to_row(self, m: Machine) -> list:
        return [
            m.machine_id, m.hostname, m.os, m.os_version, m.architecture, m.cores, m.total_memory, m.total_disk,
            m.last_boot.astimezone(timezone.utc), RowValues.utc(m.observed_at),
        ]
