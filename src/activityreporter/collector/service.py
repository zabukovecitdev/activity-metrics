import asyncio
import logging
import math
import time
from collections.abc import Callable, Iterable

import httpx
from kafka.errors import KafkaError
from prometheus_client import Counter
from pydantic import ValidationError

from activityreporter.collector.repository import (
    HttpAgentMetricsRepository,
    KafkaJsonProducer,
    KafkaMachinesRepository,
    KafkaRawMetricsRepository,
    machine_url,
    parse_machine,
    parse_metrics,
)

logger = logging.getLogger(__name__)

SCRAPES = Counter(
    "activityreporter_collector_scrapes_total",
    "Agent scrapes by outcome: ok, fetch_error (no valid response) or publish_error",
    ["outcome"],
)
INVALID_RESPONSES = Counter(
    "activityreporter_collector_invalid_responses_total",
    "Agent responses that failed validation and went to the dead letter topic, by kind: metrics or machine",
    ["kind"],
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
        metrics = await self._receive("metrics", endpoint, parse_metrics, self._raw_metrics)
        if metrics is None:
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

        machine = await self._receive("machine", machine_url(endpoint), parse_machine, self._machines)
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
            INVALID_RESPONSES.labels(kind).inc()
            try:
                await asyncio.to_thread(producer.dead_letter, url, body, str(e))
            except KafkaError:
                logger.exception("Failed to dead-letter invalid %s from %s", kind, url)
            return None
