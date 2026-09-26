from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.query import QueryResult


class ClickHouseConnector:
    def __init__(self, client: AsyncClient):
        self._client = client

    @classmethod
    async def from_env(cls) -> ClickHouseConnector:
        client = await clickhouse_connect.get_async_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "user"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "password"),
            database=os.environ.get("CLICKHOUSE_DB", "metrics"),
            # A per-client session rejects concurrent queries, and one client is
            # shared by every coroutine using this connector.
            autogenerate_session_id=False,
        )
        return cls(client)

    async def query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        return await self._client.query(query, parameters=params)

    async def insert(self, table: str, rows: Sequence[Sequence[Any]], column_names: Sequence[str]) -> None:
        await self._client.insert(table, rows, column_names=column_names)

    async def close(self) -> None:
        await self._client.close()

    async def __aenter__(self) -> ClickHouseConnector:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


async def main() -> None:
    async with await ClickHouseConnector.from_env() as connector:
        result = await connector.query("SELECT version()")
        print(result.result_rows)


if __name__ == "__main__":
    asyncio.run(main())
