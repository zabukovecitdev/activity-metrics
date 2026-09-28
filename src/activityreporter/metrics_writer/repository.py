import logging
import os
import time
from dataclasses import asdict

import psycopg
from psycopg.types.json import Jsonb

from core.metrics import ProcessedMetric

logger = logging.getLogger(__name__)

INSERT_SQL = """
    INSERT INTO metrics
        (machine_id, name, "timestamp", type, unit, value, attributes, is_anomaly)
    VALUES
        (%(machine_id)s, %(name)s, to_timestamp(%(timestamp)s),
         %(type)s, %(unit)s, %(value)s, %(attributes)s, %(is_anomaly)s)
    ON CONFLICT (machine_id, name, attributes, "timestamp") DO NOTHING
"""


class TimescaleConnector:
    def __init__(self, dsn: str, max_retries: int = 5, backoff_base_seconds: float = 1.0):
        self._dsn = dsn
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._conn: psycopg.Connection | None = None

    @classmethod
    def from_env(cls) -> TimescaleConnector:
        dsn = (
            f"host={os.environ.get('TIMESCALE_HOST', 'localhost')} "
            f"port={os.environ.get('TIMESCALE_PORT', '5432')} "
            f"dbname={os.environ.get('TIMESCALE_DB', 'activityreporter')} "
            f"user={os.environ.get('TIMESCALE_USER', 'activityreporter')} "
            f"password={os.environ.get('TIMESCALE_PASSWORD', 'activityreporter')}"
        )
        return cls(dsn=dsn)

    def connect(self) -> None:
        self._conn = psycopg.connect(self._dsn, autocommit=False)

    def insert_batch(self, metrics_batch: list[ProcessedMetric]) -> None:
        if not metrics_batch:
            return

        # None → {}: attributes is part of the primary key, so it can't be NULL.
        rows = [{**asdict(m), "attributes": Jsonb(m.attributes or {})} for m in metrics_batch]

        for attempt in range(1, self._max_retries + 1):
            try:
                with self._conn.cursor() as cursor:
                    cursor.executemany(INSERT_SQL, rows)
                self._conn.commit()
                return
            except psycopg.Error:
                logger.exception(
                    "Failed to write metrics batch to TimescaleDB (attempt %d/%d)",
                    attempt,
                    self._max_retries,
                )
                self._reset_connection()
                if attempt == self._max_retries:
                    raise
                time.sleep(self._backoff_base_seconds * (2 ** (attempt - 1)))

    def _reset_connection(self) -> None:
        try:
            if self._conn is not None and not self._conn.closed:
                self._conn.rollback()
                return
        except psycopg.Error:
            logger.exception("Rollback failed, reconnecting to TimescaleDB")
        self.connect()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()

    def __enter__(self) -> TimescaleConnector:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()
