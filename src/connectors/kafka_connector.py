import json
import logging
import os
from dataclasses import asdict

from kafka import KafkaProducer
from kafka.errors import KafkaError

from client.metric_factory import Metrics

logger = logging.getLogger(__name__)


class KafkaConnector:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    @classmethod
    def from_env(cls) -> KafkaConnector:
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            topic=os.environ.get("KAFKA_METRICS_TOPIC", "metrics"),
        )

    def send(self, metrics: Metrics) -> None:
        try:
            future = self._producer.send(self._topic, value=asdict(metrics), key=metrics.machine_id)
            future.get(timeout=10)
        except KafkaError:
            logger.exception("Failed to send metrics to Kafka")
            raise

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close()

    def __enter__(self) -> KafkaConnector:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
