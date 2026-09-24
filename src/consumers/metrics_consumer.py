import json
import logging
import os
import time

from kafka import KafkaConsumer

from client.metric_factory import Metrics
from connectors.timescale_connector import TimescaleConnector

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0
POLL_TIMEOUT_MS = 1000


class MetricsConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        writer: TimescaleConnector,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._writer = writer
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    @classmethod
    def from_env(cls, writer: TimescaleConnector) -> MetricsConsumer:
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            topic=os.environ.get("KAFKA_PROCESSED_METRICS_TOPIC", "processed_metrics"),
            group_id=os.environ.get("KAFKA_CONSUMER_GROUP_ID", "timescale-writer"),
            writer=writer,
            batch_size=int(os.environ.get("CONSUMER_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            batch_timeout_seconds=float(
                os.environ.get("CONSUMER_BATCH_TIMEOUT_SECONDS", DEFAULT_BATCH_TIMEOUT_SECONDS)
            ),
        )

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        batch: list[Metrics] = []
        last_flush = time.monotonic()
        try:
            while self._running:
                polled = self._consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=self._batch_size)
                for records in polled.values():
                    batch.extend(Metrics(**record.value) for record in records)

                if batch and (
                    len(batch) >= self._batch_size
                    or time.monotonic() - last_flush >= self._batch_timeout_seconds
                ):
                    self._flush(batch)
                    batch = []
                    last_flush = time.monotonic()

            if batch:
                self._flush(batch)
        finally:
            self._consumer.close()

    def _flush(self, batch: list[Metrics]) -> None:
        # Offsets are committed only after a successful write, so a crash here
        # replays the batch on restart; the insert is idempotent via ON CONFLICT.
        self._writer.insert_batch(batch)
        self._consumer.commit()
        logger.info("Wrote %d metrics to TimescaleDB and committed offsets", len(batch))
