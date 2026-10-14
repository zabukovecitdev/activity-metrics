import logging
import time
from typing import Self

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from activityreporter.shared.settings.clickhouse_settings import ClickHouseSettings

logger = logging.getLogger(__name__)


class ClickHouseRepository:
    def __init__(self, settings: ClickHouseSettings):
        self._settings = settings
        self._client: Client | None = None

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(
            host=self._settings.host,
            port=self._settings.port,
            username=self._settings.user,
            password=self._settings.password.get_secret_value(),
            database=self._settings.db,
        )

    def insert_batch(self, table: str, columns: list[str], rows: list[list]) -> None:
        if not rows:
            return

        for attempt in range(1, self._settings.max_retries + 1):
            try:
                assert self._client is not None
                self._client.insert(table, rows, column_names=columns)
                return
            except ClickHouseError:
                logger.exception(
                    "Failed to write %s batch to ClickHouse (attempt %d/%d)",
                    table,
                    attempt,
                    self._settings.max_retries,
                )
                if attempt == self._settings.max_retries:
                    raise
                time.sleep(self._settings.backoff_base_seconds * (2 ** (attempt - 1)))

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()
