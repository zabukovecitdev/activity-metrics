from __future__ import annotations

from activityreporter.collector.producers.kafka_json_producer import KafkaJsonProducer
from activityreporter.shared.models.machine import Machine
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class KafkaMachinesProducer(KafkaJsonProducer):
    @classmethod
    def from_settings(cls, kafka: KafkaSettings) -> KafkaMachinesProducer:
        return cls(bootstrap_servers=kafka.connection_string, topic=kafka.machines_topic)

    def publish(self, machine: Machine) -> None:
        self._publish([machine])
