import asyncio
import logging
import math
import time
from collections.abc import Callable, Iterable

import httpx
from kafka.errors import KafkaError
from pydantic import ValidationError

from activityreporter.collector.agents.agent_urls import AgentUrls
from activityreporter.collector.clients.http_agent_client import HttpAgentClient
from activityreporter.collector.parsers.machine_parser import MachineParser
from activityreporter.collector.parsers.metrics_parser import MetricsParser
from activityreporter.collector.producers.kafka_json_producer import KafkaJsonProducer
from activityreporter.collector.producers.kafka_machines_producer import KafkaMachinesProducer
from activityreporter.collector.producers.kafka_metrics_producer import KafkaMetricsProducer
from activityreporter.shared.observability.metrics import Metrics
from activityreporter.shared.settings.collector_settings import CollectorSettings

logger = logging.getLogger(__name__)


class Collector:
    def __init__(
        self,
        endpoints: Iterable[str],
        agent_metrics: HttpAgentClient,
        raw_metrics: KafkaMetricsProducer,
        machines: KafkaMachinesProducer,
        settings: CollectorSettings,
    ):
        self._endpoints = set(endpoints)
        self._agent_metrics = agent_metrics
        self._raw_metrics = raw_metrics
        self._machines = machines
        self._settings = settings
        self._machine_refreshed_at: dict[str, float] = {}

    async def run(self) -> None:
        while True:
            await self.collect()
            await asyncio.sleep(self._settings.scrape_interval_seconds)

    async def collect(self) -> None:
        await asyncio.gather(*(self._scrape(endpoint) for endpoint in self._endpoints))

    async def _scrape(self, endpoint: str) -> None:
        await self._refresh_machine(endpoint)
        metrics = await self._receive("metrics", endpoint, MetricsParser.parse, self._raw_metrics)
        if metrics is None:
            Metrics.SCRAPES.labels("fetch_error").inc()
            return

        try:
            await asyncio.to_thread(self._raw_metrics.publish, metrics)
        except KafkaError:
            logger.exception("Failed to publish metrics from %s to Kafka", endpoint)
            Metrics.SCRAPES.labels("publish_error").inc()
        else:
            Metrics.SCRAPES.labels("ok").inc()

    async def _refresh_machine(self, endpoint: str) -> None:
        now = time.monotonic()
        if now - self._machine_refreshed_at.get(endpoint, -math.inf) < self._settings.machine_refresh_seconds:
            return

        machine = await self._receive("machine", AgentUrls.machine_url(endpoint), MachineParser.parse, self._machines)
        if machine is None:
            return

        try:
            await asyncio.to_thread(self._machines.publish, machine)
        except KafkaError:
            logger.exception("Failed to publish machine info from %s to Kafka", endpoint)
            return
        # Only after a successful publish, so a failure is retried on the next scrape.
        self._machine_refreshed_at[endpoint] = now

    async def _receive[T](self, kind: str, url: str, parse: Callable[[str], T], producer: KafkaJsonProducer) -> T | None:
        """`parse` applied to the body at `url`, or None once the failure is logged.

        The one place agent responses are validated: an invalid one is counted and sent to `producer`'s dead letter
        topic as it was received.
        """
        try:
            body = await self._agent_metrics.fetch(url)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch %s from %s: %s", kind, url, e)
            return None

        try:
            return parse(body)
        except ValidationError as e:
            logger.error("Invalid %s from %s: %s", kind, url, e)
            Metrics.INVALID_RESPONSES.labels(kind).inc()
            try:
                await asyncio.to_thread(producer.dead_letter, url, body, str(e))
            except KafkaError:
                logger.exception("Failed to dead-letter invalid %s from %s", kind, url)
            return None
