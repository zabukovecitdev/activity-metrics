import asyncio
import logging
import math
import time
from collections.abc import Iterable

import httpx
from kafka.errors import KafkaError
from prometheus_client import Counter

from activityreporter.collector.repository import (
    HttpAgentMetricsRepository,
    KafkaMachinesRepository,
    KafkaRawMetricsRepository,
)

logger = logging.getLogger(__name__)

SCRAPES = Counter(
    "activityreporter_collector_scrapes_total",
    "Agent scrapes by outcome: ok, fetch_error or publish_error",
    ["outcome"],
)

SCRAPE_INTERVAL_SECONDS = 10
MACHINE_REFRESH_SECONDS = 300


class Collector:
    def __init__(
        self,
        endpoints: Iterable[str],
        agent_metrics: HttpAgentMetricsRepository,
        raw_metrics: KafkaRawMetricsRepository,
        machines: KafkaMachinesRepository,
    ):
        self._endpoints = set(endpoints)
        self._agent_metrics = agent_metrics
        self._raw_metrics = raw_metrics
        self._machines = machines
        self._machine_refreshed_at: dict[str, float] = {}

    async def run(self) -> None:
        while True:
            await self.collect()
            await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)

    async def collect(self) -> None:
        await asyncio.gather(*(self._scrape(endpoint) for endpoint in self._endpoints))

    async def _scrape(self, endpoint: str) -> None:
        await self._refresh_machine(endpoint)
        try:
            metrics = await self._agent_metrics.fetch(endpoint)
        except httpx.HTTPError as e:
            logger.error("Failed to scrape metrics from %s: %s", endpoint, e)
            SCRAPES.labels("fetch_error").inc()
            return
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Malformed response from %s: %r", endpoint, e)
            SCRAPES.labels("fetch_error").inc()
            return

        try:
            await asyncio.to_thread(self._raw_metrics.publish, metrics)
        except KafkaError:
            logger.exception("Failed to publish metrics from %s to Kafka", endpoint)
            SCRAPES.labels("publish_error").inc()
        else:
            SCRAPES.labels("ok").inc()

    async def _refresh_machine(self, endpoint: str) -> None:
        now = time.monotonic()
        if now - self._machine_refreshed_at.get(endpoint, -math.inf) < MACHINE_REFRESH_SECONDS:
            return

        try:
            machine = await self._agent_metrics.fetch_machine(endpoint)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch machine info from %s: %s", endpoint, e)
            return
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Malformed machine info from %s: %r", endpoint, e)
            return

        try:
            await asyncio.to_thread(self._machines.publish, machine)
        except KafkaError:
            logger.exception("Failed to publish machine info from %s to Kafka", endpoint)
            return
        # Only after a successful publish, so a failure is retried on the next scrape.
        self._machine_refreshed_at[endpoint] = now
