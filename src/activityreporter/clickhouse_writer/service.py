from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import fields
from typing import Any, ClassVar

from prometheus_client import Counter

from activityreporter.clickhouse_writer.repository import ClickHouseRepository, KafkaRecordsRepository
from activityreporter.shared.settings import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_BATCH_TIMEOUT_SECONDS,
    KafkaSettings,
    WriterSettings,
)

logger = logging.getLogger(__name__)

ROWS_WRITTEN = Counter(
    "activityreporter_writer_rows_written_total", "Rows written to ClickHouse", ["table"]
)
MALFORMED = Counter(
    "activityreporter_writer_malformed_records_total", "Kafka records skipped as malformed", ["table"]
)


def from_json(record_type: type, data: dict[str, Any]):
    """`record_type` built from `data`, ignoring keys it doesn't know.

    A producer may add fields before every consumer is deployed with them.
    """
    if not isinstance(data, dict):
        raise TypeError(f"expected a JSON object, got {type(data).__name__}")
    known = {f.name for f in fields(record_type)}
    return record_type(**{k: v for k, v in data.items() if k in known})


class TopicWriter(ABC):
    """Writes one Kafka topic into one ClickHouse table: batches rows, then writes them before committing offsets.

    A subclass names the table, its columns, the record dataclass, the topic and how a record becomes a row.
    The topic comes from `KafkaSettings`, the same setting the producer of that topic uses. Run several instances for one topic and Kafka splits its partitions
    between them, because they share a consumer group.
    """

    table: ClassVar[str]
    columns: ClassVar[list[str]]
    record_type: ClassVar[type]

    def __init__(
        self,
        records: KafkaRecordsRepository,
        store: ClickHouseRepository,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._records = records
        self._store = store
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True

    @classmethod
    @abstractmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        """The Kafka topic this writer consumes."""

    @classmethod
    def group_id(cls) -> str:
        return f"clickhouse-{cls.table}-writer"

    @classmethod
    def from_settings(
        cls, records: KafkaRecordsRepository, store: ClickHouseRepository, settings: WriterSettings
    ) -> TopicWriter:
        return cls(
            records,
            store,
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
        parsed = (self._parse(record) for record in self._records.poll(max_records=self._batch_size))
        return [row for row in parsed if row is not None]

    def _parse(self, record) -> list | None:
        # Converting here, not at insert time, keeps a bad record from failing every retry of its batch.
        try:
            return self.to_row(from_json(self.record_type, json.loads(record.value)))
        except (TypeError, ValueError) as e:
            logger.error(
                "Skipping malformed record %s[%s]@%s: %r", record.topic, record.partition, record.offset, e
            )
            MALFORMED.labels(self.table).inc()
            return None

    def _flush(self, rows: list[list]) -> None:
        # A crash before the commit replays the batch, and ReplacingMergeTree collapses the duplicates.
        self._store.insert_batch(self.table, self.columns, rows)
        self._records.commit()
        ROWS_WRITTEN.labels(self.table).inc(len(rows))
        logger.info("Wrote %d %s rows to ClickHouse and committed offsets", len(rows), self.table)
