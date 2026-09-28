from __future__ import annotations

import logging
import os
import time

from activityreporter.metrics_writer.repository import KafkaProcessedMetricsRepository, ClickHouseMetricsRepository
from activityreporter.shared.metrics import ProcessedMetric

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class MetricsWriter:
    def __init__(
        self,
        processed_metrics: KafkaProcessedMetricsRepository,
        metrics_store: ClickHouseMetricsRepository,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._processed_metrics = processed_metrics
        self._metrics_store = metrics_store
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True

    @classmethod
    def from_env(
        cls, processed_metrics: KafkaProcessedMetricsRepository, metrics_store: ClickHouseMetricsRepository
    ) -> MetricsWriter:
        return cls(
            processed_metrics,
            metrics_store,
            batch_size=int(os.environ.get("METRICS_WRITER_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            batch_timeout_seconds=float(
                os.environ.get("METRICS_WRITER_BATCH_TIMEOUT_SECONDS", DEFAULT_BATCH_TIMEOUT_SECONDS)
            ),
        )

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        batch: list[ProcessedMetric] = []
        last_flush = time.monotonic()
        while self._running:
            batch.extend(self._processed_metrics.poll(max_records=self._batch_size))
            if batch and (
                len(batch) >= self._batch_size
                or time.monotonic() - last_flush >= self._batch_timeout_seconds
            ):
                self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        if batch:
            self._flush(batch)

    def _flush(self, batch: list[ProcessedMetric]) -> None:
        # Commit only after the write: a crash replays the batch, and ReplacingMergeTree collapses the duplicates.
        self._metrics_store.insert_batch(batch)
        self._processed_metrics.commit()
        logger.info("Wrote %d metrics to ClickHouse and committed offsets", len(batch))
