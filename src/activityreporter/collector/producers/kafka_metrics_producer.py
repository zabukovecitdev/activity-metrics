from __future__ import annotations

from collections.abc import Iterable

from activityreporter.collector.producers.kafka_producer_base import KafkaProducerBase
from activityreporter.shared.models.metric import Metric
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class KafkaMetricsProducer(KafkaProducerBase):
    @classmethod
    def from_settings(cls, kafka: KafkaSettings) -> KafkaMetricsProducer:
        return cls(bootstrap_servers=kafka.connection_string, topic=kafka.raw_metrics_topic)

    def publish(self, metrics: Iterable[Metric]) -> None:
        self._publish(metrics)
