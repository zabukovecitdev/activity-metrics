from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

import httpx
from kafka.errors import KafkaError

from connectors.kafka_connector import KafkaConnector
from core.metrics import Metric

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_SECONDS = 10
CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2
MAX_CONNECTIONS = 200
MAX_KEEPALIVE_CONNECTIONS = 200


class Collector:
    def __init__(self, endpoints: Callable[[], Iterable[str]], connector: KafkaConnector):
        # Called on every scrape, so clients discovered or lost in between are picked up.
        self.endpoints = endpoints
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
        await asyncio.gather(*(self._scrape(endpoint) for endpoint in set(self.endpoints())))

    async def _scrape(self, endpoint: str) -> None:
        try:
            response = await self._client.get(endpoint)
            response.raise_for_status()
            metrics = parse_metrics(response.json())
        except httpx.HTTPError as e:
            logger.error("Failed to scrape metrics from %s: %s", endpoint, e)
            return
        except (KeyError, TypeError, ValueError) as e:
            # Caught here so one misbehaving client can't take down the whole gather.
            logger.error("Malformed response from %s: %r", endpoint, e)
            return

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


def parse_metrics(payload: dict[str, Any]) -> list[Metric]:
    machine_id = payload["machine_id"]
    timestamp = datetime.fromisoformat(payload["timestamp"]).timestamp()
    return [
        Metric(
            timestamp=timestamp,
            name=m["name"],
            type=m["type"],
            unit=m["unit"],
            value=float(m["value"]),
            machine_id=machine_id,
            attributes=m.get("attributes"),
        )
        for m in payload["metrics"]
    ]
