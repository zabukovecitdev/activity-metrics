from collections.abc import Iterable
from typing import Self

from kafka import KafkaProducer
from pydantic_core import to_json

from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric import Metric

SEND_TIMEOUT_SECONDS = 10


class KafkaJsonProducer:
    """JSON records to one Kafka topic, keyed by machine id, and invalid input to `<topic>_dlq`."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._dlq_topic = f"{topic}_dlq"
        self._producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=to_json,
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    def _publish(self, records: Iterable[Metric | Machine]) -> None:
        queued = [self._producer.send(self._topic, value=r, key=r.machine_id) for r in records]
        for future in queued:
            future.get(timeout=SEND_TIMEOUT_SECONDS)

    def dead_letter(self, url: str, body: str, error: str) -> None:
        # Keyed by the agent URL: an invalid body may have no machine id to key it by.
        record = {"url": url, "error": error, "body": body}
        self._producer.send(self._dlq_topic, value=record, key=url).get(timeout=SEND_TIMEOUT_SECONDS)

    def close(self) -> None:
        self._producer.flush(timeout=SEND_TIMEOUT_SECONDS)
        self._producer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
