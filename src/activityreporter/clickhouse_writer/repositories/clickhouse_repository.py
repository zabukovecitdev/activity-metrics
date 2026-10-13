from __future__ import annotations

import logging
import time
from typing import Self

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from activityreporter.shared.settings.clickhouse_settings import ClickHouseSettings

logger = logging.getLogger(__name__)


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
        self._connection = {"host": host, "port": port, "username": username, "password": password, "database": database}
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._client: Client | None = None

    @classmethod
    def from_settings(cls, settings: ClickHouseSettings) -> ClickHouseRepository:
        return cls(
            host=settings.host,
            port=settings.port,
            username=settings.user,
            password=settings.password.get_secret_value(),
            database=settings.db,
        )

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(**self._connection)

    def insert_batch(self, table: str, columns: list[str], rows: list[list]) -> None:
        if not rows:
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                assert self._client is not None
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

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()
