from typing import Self

import httpx

from activityreporter.shared.settings.http_agent_client_settings import (
    HttpAgentClientSettings,
)


class HttpAgentClient:
    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        settings: HttpAgentClientSettings | None = None,
    ):
        """Creates the HTTP client.

        Args:
            client: The HTTP client to use. When it is None, a client is built from `settings`.
            settings: The timeouts and connection limit. When it is None, the values come from the environment.
        """
        if client is None:
            settings = settings or HttpAgentClientSettings()
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    settings.read_timeout_seconds,
                    connect=settings.connect_timeout_seconds,
                ),
                limits=httpx.Limits(
                    max_connections=settings.max_connections,
                    max_keepalive_connections=settings.max_connections,
                ),
            )
        self._client = client

    async def fetch(self, url: str) -> str:
        """The raw body at `url`, unparsed, so an invalid one can be dead-lettered as it was received."""
        response = await self._client.get(url)
        response.raise_for_status()
        return response.text

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc) -> None:
        await self._client.aclose()
