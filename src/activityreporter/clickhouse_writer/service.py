from __future__ import annotations

import logging
import os
import time

from activityreporter.clickhouse_writer.repository import ClickHouseRepository, KafkaRecordsRepository

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class ClickHouseWriter:
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
    def from_env(cls, records: KafkaRecordsRepository, store: ClickHouseRepository) -> ClickHouseWriter:
        return cls(
            records,
            store,
            batch_size=int(os.environ.get("WRITER_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            batch_timeout_seconds=float(
                os.environ.get("WRITER_BATCH_TIMEOUT_SECONDS", DEFAULT_BATCH_TIMEOUT_SECONDS)
            ),
        )

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        batch: list = []
        last_flush = time.monotonic()
        while self._running:
            batch.extend(self._records.poll(max_records=self._batch_size))
            if batch and (
                len(batch) >= self._batch_size
                or time.monotonic() - last_flush >= self._batch_timeout_seconds
            ):
                self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        if batch:
            self._flush(batch)

    def _flush(self, batch: list) -> None:
        # Commit only after the write: a crash replays the batch, and ReplacingMergeTree collapses the duplicates.
        self._store.insert_batch(batch)
        self._records.commit()
        logger.info("Wrote %d records to ClickHouse and committed offsets", len(batch))
