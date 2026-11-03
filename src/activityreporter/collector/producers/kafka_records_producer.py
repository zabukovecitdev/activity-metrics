from collections.abc import Iterable
from typing import Self

from kafka import KafkaProducer
from pydantic_core import to_json

from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric import Metric
from activityreporter.shared.observability.metrics import Metrics

SEND_TIMEOUT_SECONDS = 10


class KafkaRecordsProducer:
    """Sends records as JSON to a Kafka topic.

    The producer uses the machine id as the key for each record.
    The producer sends input that is not valid to the `<topic>_dlq` topic.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        """Creates a Kafka producer for the topic and its dead letter topic.

        Args:
            bootstrap_servers: The address of the Kafka broker, for example `localhost:9092`.
            topic: The name of the topic for valid records.
        """
        self._topic = topic
        self._dlq_topic = f"{topic}_dlq"
        self._producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=to_json,
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def publish(self, records: Iterable[Metric | Machine]) -> None:
        """Sends the records to the topic and waits until Kafka accepts all of them.

        Args:
            records: The metrics or machines to send.

        Raises:
            kafka.errors.KafkaError: Kafka did not accept a record in the time limit.
        """
        queued = [
            self._producer.send(self._topic, value=r, key=r.machine_id) for r in records
        ]
        for future in queued:
            future.get(timeout=SEND_TIMEOUT_SECONDS)
        Metrics.MESSAGES_PUBLISHED.labels(self._topic).inc(len(queued))

    def dead_letter(self, url: str, body: str, error: str) -> None:
        """Sends input that is not valid to the dead letter topic.

        Args:
            url: The agent URL that gave the input.
            body: The raw response body.
            error: The text of the validation error.

        Raises:
            kafka.errors.KafkaError: Kafka did not accept the record in the time limit.
        """
        # Keyed by the agent URL: an invalid body may have no machine id to key it by.
        record = {"url": url, "error": error, "body": body}
        self._producer.send(self._dlq_topic, value=record, key=url).get(
            timeout=SEND_TIMEOUT_SECONDS
        )
        Metrics.MESSAGES_PUBLISHED.labels(self._dlq_topic).inc()

    def close(self) -> None:
        self._producer.flush(timeout=SEND_TIMEOUT_SECONDS)
        self._producer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
