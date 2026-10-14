from typing import Self

from kafka import KafkaConsumer


class KafkaRecordsConsumer:
    """One consumer on one topic."""

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str, poll_timeout_ms: int):
        self._poll_timeout_ms = poll_timeout_ms
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    def poll(self, max_records: int) -> list:
        polled = self._consumer.poll(timeout_ms=self._poll_timeout_ms, max_records=max_records)
        return [record for records in polled.values() for record in records]

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
