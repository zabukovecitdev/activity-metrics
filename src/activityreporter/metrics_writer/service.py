from __future__ import annotations

import logging
import os
import time

from activityreporter.metrics_writer.repository import KafkaProcessedMetricsRepository, TimescaleMetricsRepository
from activityreporter.shared.metrics import ProcessedMetric

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class MetricsWriter:
    def __init__(
        self,
        processed_metrics: KafkaProcessedMetricsRepository,
        timescale_metrics: TimescaleMetricsRepository,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS,
    ):
        self._processed_metrics = processed_metrics
        self._timescale_metrics = timescale_metrics
        self._batch_size = batch_size
        self._batch_timeout_seconds = batch_timeout_seconds
        self._running = True

    @classmethod
    def from_env(
        cls, processed_metrics: KafkaProcessedMetricsRepository, timescale_metrics: TimescaleMetricsRepository
    ) -> MetricsWriter:
        return cls(
            processed_metrics,
            timescale_metrics,
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
        # Commit only after the write: a crash replays the batch, and the insert is idempotent via ON CONFLICT.
        self._timescale_metrics.insert_batch(batch)
        self._processed_metrics.commit()
        logger.info("Wrote %d metrics to TimescaleDB and committed offsets", len(batch))
