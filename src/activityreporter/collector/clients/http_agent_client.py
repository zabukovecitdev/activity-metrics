from typing import Self

import httpx

CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2
MAX_CONNECTIONS = 200


class HttpAgentClient:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(READ_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS),
            limits=httpx.Limits(max_connections=MAX_CONNECTIONS, max_keepalive_connections=MAX_CONNECTIONS),
        )

    async def fetch(self, url: str) -> str:
        """The raw body at `url`, unparsed, so an invalid one can be dead-lettered as it was received."""
        response = await self._client.get(url)
        response.raise_for_status()
        return response.text

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc) -> None:
        await self._client.aclose()
