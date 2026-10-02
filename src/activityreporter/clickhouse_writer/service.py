from __future__ import annotations

import logging
import os
import time

from activityreporter.clickhouse_writer.repository import ClickHouseRepository, KafkaRecordsRepository, Sink

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class ClickHouseWriter:
    """Batches rows per table and writes every table before committing the offsets they came from."""

    def __init__(
        self,
        records: KafkaRecordsRepository,
        store: ClickHouseRepository,
        sinks: list[Sink],
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._records = records
        self._store = store
        self._sinks = {sink.table: sink for sink in sinks}
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True

    @classmethod
    def from_env(
        cls, records: KafkaRecordsRepository, store: ClickHouseRepository, sinks: list[Sink]
    ) -> ClickHouseWriter:
        return cls(
            records,
            store,
            sinks,
            batch_size=int(os.environ.get("WRITER_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            batch_timeout_seconds=float(
                os.environ.get("WRITER_BATCH_TIMEOUT_SECONDS", DEFAULT_BATCH_TIMEOUT_SECONDS)
            ),
        )

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        batches: dict[str, list] = {}
        last_flush = time.monotonic()
        while self._running:
            for table, rows in self._records.poll(max_records=self._batch_size).items():
                batches.setdefault(table, []).extend(rows)
            if batches and (
                any(len(rows) >= self._batch_size for rows in batches.values())
                or time.monotonic() - last_flush >= self._batch_timeout_seconds
            ):
                self._flush(batches)
                batches = {}
                last_flush = time.monotonic()

        if batches:
            self._flush(batches)

    def _flush(self, batches: dict[str, list]) -> None:
        # Offsets are per consumer, not per table, so every table is written before the commit.
        # A crash replays all of them, and ReplacingMergeTree collapses the duplicates.
        for table, rows in batches.items():
            self._store.insert_batch(self._sinks[table], rows)
        self._records.commit()
        logger.info(
            "Wrote %s to ClickHouse and committed offsets",
            ", ".join(f"{len(rows)} {table}" for table, rows in batches.items()),
        )
