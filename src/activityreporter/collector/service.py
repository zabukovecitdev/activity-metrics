import asyncio
import logging
from collections.abc import Callable, Iterable

import httpx
from kafka.errors import KafkaError

from activityreporter.collector.repository import HttpAgentMetricsRepository, KafkaRawMetricsRepository

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_SECONDS = 10


class Collector:
    def __init__(
        self,
        list_endpoints: Callable[[], Iterable[str]],
        agent_metrics: HttpAgentMetricsRepository,
        raw_metrics: KafkaRawMetricsRepository,
    ):
        self._list_endpoints = list_endpoints
        self._agent_metrics = agent_metrics
        self._raw_metrics = raw_metrics

    async def run(self) -> None:
        while True:
            await self.collect()
            await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)

    async def collect(self) -> None:
        await asyncio.gather(*(self._scrape(endpoint) for endpoint in set(self._list_endpoints())))

    async def _scrape(self, endpoint: str) -> None:
        try:
            metrics = await self._agent_metrics.fetch(endpoint)
        except httpx.HTTPError as e:
            logger.error("Failed to scrape metrics from %s: %s", endpoint, e)
            return
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Malformed response from %s: %r", endpoint, e)
            return

        try:
            await asyncio.to_thread(self._raw_metrics.publish, metrics)
        except KafkaError:
            logger.exception("Failed to publish metrics from %s to Kafka", endpoint)
