from __future__ import annotations

import asyncio
import logging

import httpx

from client.metric_factory import Metrics

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_SECONDS = 15
CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 5
MAX_CONNECTIONS = 200
MAX_KEEPALIVE_CONNECTIONS = 200


class Collector:
    def __init__(self, endpoint_urls: list[str]):
        self.endpoints = endpoint_urls
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(READ_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS),
            limits=httpx.Limits(
                max_connections=MAX_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
            ),
        )

    async def run(self) -> None:
        while True:
            await self.collect()
            await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)

    async def collect(self) -> None:
        await asyncio.gather(*(self._scrape(endpoint) for endpoint in self.endpoints))

    async def _scrape(self, endpoint: str) -> None:
        try:
            response = await self._client.get(endpoint)
            response.raise_for_status()
            metrics = Metrics(**response.json())
            print(metrics)
        except httpx.HTTPError as e:
            logger.error("Zajem metrik iz %s ni uspel: %s", endpoint, e)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Collector:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()
