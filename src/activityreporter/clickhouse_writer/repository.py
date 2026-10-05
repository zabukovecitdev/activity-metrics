from __future__ import annotations

import logging
import os
import time

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

POLL_TIMEOUT_MS = 1000


class ClickHouseRepository:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
    ):
        self._connection = dict(host=host, port=port, username=username, password=password, database=database)
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._client: Client | None = None

    @classmethod
    def from_env(cls) -> ClickHouseRepository:
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "user"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "password"),
            database=os.environ.get("CLICKHOUSE_DB", "metrics"),
        )

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(**self._connection)

    def insert_batch(self, table: str, columns: list[str], rows: list[list]) -> None:
        if not rows:
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                self._client.insert(table, rows, column_names=columns)
                return
            except ClickHouseError:
                logger.exception(
                    "Failed to write %s batch to ClickHouse (attempt %d/%d)",
                    table,
                    attempt,
                    self._max_retries,
                )
                if attempt == self._max_retries:
                    raise
                time.sleep(self._backoff_base_seconds * (2 ** (attempt - 1)))

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> ClickHouseRepository:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class KafkaRecordsRepository:
    """One consumer on one topic."""

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    @classmethod
    def from_env(cls, topic: str, group_id: str) -> KafkaRecordsRepository:
        return cls(topic, os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"), group_id)

    def poll(self, max_records: int) -> list:
        polled = self._consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=max_records)
        return [record for records in polled.values() for record in records]

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> KafkaRecordsRepository:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
