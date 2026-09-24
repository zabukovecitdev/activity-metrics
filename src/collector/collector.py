from __future__ import annotations

import asyncio
import logging

import httpx
from kafka.errors import KafkaError

from connectors.kafka_connector import KafkaConnector
from core.metrics import RawMetrics

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_SECONDS = 1
CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2
MAX_CONNECTIONS = 200
MAX_KEEPALIVE_CONNECTIONS = 200


class Collector:
    def __init__(self, endpoint_urls: list[str], connector: KafkaConnector):
        self.endpoints = endpoint_urls
        self.connector = connector
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
        except httpx.HTTPError as e:
            logger.error("Failed to scrape metrics from %s: %s", endpoint, e)
            return

        metrics = RawMetrics(**response.json())
        print(metrics)

        try:
            # offloaded to a thread: KafkaProducer.send()/future.get() block,
            # and would otherwise stall the event loop for every other scrape in flight
            await asyncio.to_thread(self.connector.send, metrics)
        except KafkaError:
            pass  # already logged with a stack trace inside connector.send

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Collector:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()
