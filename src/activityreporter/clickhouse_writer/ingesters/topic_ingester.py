from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from activityreporter.clickhouse_writer.consumers.kafka_records_consumer import KafkaRecordsConsumer
from activityreporter.clickhouse_writer.ingesters.record_parser import RecordParser
from activityreporter.clickhouse_writer.repositories.clickhouse_repository import ClickHouseRepository
from activityreporter.shared.observability.metrics import Metrics
from activityreporter.shared.settings.ingester_settings import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_BATCH_TIMEOUT_SECONDS,
    IngesterSettings,
)
from activityreporter.shared.settings.kafka_settings import KafkaSettings

logger = logging.getLogger(__name__)


class TopicIngester(ABC):
    """Loads one Kafka topic into one ClickHouse table: batches rows, then writes them before committing offsets.

    A subclass names the table, its columns, the record model, the topic and how a record becomes a row.
    The topic comes from `KafkaSettings`, the same setting the producer of that topic uses. Run several instances for one topic and Kafka splits its partitions
    between them, because they share a consumer group.
    """

    table: ClassVar[str]
    columns: ClassVar[list[str]]
    record_type: ClassVar[type]

    def __init__(
        self,
        consumer: KafkaRecordsConsumer,
        repository: ClickHouseRepository,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._consumer = consumer
        self._repository = repository
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True

    @classmethod
    @abstractmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        """The Kafka topic this ingester consumes."""

    @classmethod
    def group_id(cls) -> str:
        return f"clickhouse-{cls.table}-writer"

    @classmethod
    def from_settings(
        cls, consumer: KafkaRecordsConsumer, repository: ClickHouseRepository, settings: IngesterSettings
    ) -> TopicIngester:
        return cls(
            consumer,
            repository,
            batch_size=settings.batch_size,
            batch_timeout_seconds=settings.batch_timeout_seconds,
        )

    @abstractmethod
    def to_row(self, record: Any) -> list:
        """The ClickHouse row for `record`, in `columns` order. Raises ValueError or TypeError if it is invalid."""

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        rows: list[list] = []
        last_flush = time.monotonic()
        while self._running:
            rows.extend(self._poll())
            if rows and (
                len(rows) >= self._batch_size or time.monotonic() - last_flush >= self._batch_timeout_seconds
            ):
                self._flush(rows)
                rows = []
                last_flush = time.monotonic()

        if rows:
            self._flush(rows)

    def _poll(self) -> list[list]:
        parsed = (self._parse(record) for record in self._consumer.poll(max_records=self._batch_size))
        return [row for row in parsed if row is not None]

    def _parse(self, record) -> list | None:
        # Converting here, not at insert time, keeps a bad record from failing every retry of its batch.
        try:
            return self.to_row(RecordParser.parse(self.record_type, json.loads(record.value)))
        except (TypeError, ValueError) as e:
            logger.error(
                "Skipping malformed record %s[%s]@%s: %r", record.topic, record.partition, record.offset, e
            )
            Metrics.MALFORMED.labels(self.table).inc()
            return None

    def _flush(self, rows: list[list]) -> None:
        # A crash before the commit replays the batch, and ReplacingMergeTree collapses the duplicates.
        self._repository.insert_batch(self.table, self.columns, rows)
        self._consumer.commit()
        Metrics.ROWS_WRITTEN.labels(self.table).inc(len(rows))
        logger.info("Wrote %d %s rows to ClickHouse and committed offsets", len(rows), self.table)
