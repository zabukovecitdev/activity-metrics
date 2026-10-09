from __future__ import annotations

import tomllib
import uuid
from collections.abc import Iterable
from typing import Any, Self
from urllib.parse import urljoin, urlparse

import httpx
from kafka import KafkaProducer
from pydantic_core import to_json

from activityreporter.shared.models import Machine, Metric, MetricsResponse
from activityreporter.shared.settings import KafkaSettings

CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2
MAX_CONNECTIONS = 200
SEND_TIMEOUT_SECONDS = 10
METRICS_PATH = "/v1/metrics"
MACHINE_PATH = "/v1/machine"


class HttpAgentMetricsRepository:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(READ_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS),
            limits=httpx.Limits(max_connections=MAX_CONNECTIONS, max_keepalive_connections=MAX_CONNECTIONS),
        )

    async def fetch(self, endpoint: str) -> list[Metric]:
        response = await self._client.get(endpoint)
        response.raise_for_status()
        return parse_metrics(response.json())

    async def fetch_machine(self, endpoint: str) -> Machine:
        """The machine behind the metrics URL `endpoint`, from the same agent's /v1/machine."""
        response = await self._client.get(machine_url(endpoint))
        response.raise_for_status()
        return parse_machine(response.json())

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc) -> None:
        await self._client.aclose()


def parse_metrics(payload: dict[str, Any]) -> list[Metric]:
    # A response that doesn't match MetricsResponse is reported as malformed (ValidationError is a ValueError).
    response = MetricsResponse.model_validate(payload)
    timestamp = response.timestamp.timestamp()
    return [
        Metric(
            **m.model_dump(),
            timestamp=timestamp,
            machine_id=response.machine_id,
            metric_id=str(uuid.uuid7()),
        )
        for m in response.metrics
    ]


def load_agents(path: str) -> list[str]:
    """Base URLs of the agents to scrape, from `agents` in the TOML file at `path`.

    Raises OSError if the file can't be read and ValueError, naming the problem, if it isn't a non-empty list of
    http(s) URLs.
    """
    try:
        with open(path, "rb") as f:
            config = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"{path}: {e}") from e

    agents = config.get("agents")
    if not isinstance(agents, list) or not agents:
        raise ValueError(f"{path}: agents must be a non-empty list of URLs")
    for i, agent in enumerate(agents):
        parsed = urlparse(agent) if isinstance(agent, str) else None
        if parsed is None or parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"{path}: agents[{i}] {agent!r} must start with http:// or https://")
    return agents


def metrics_url(agent_url: str) -> str:
    return urljoin(agent_url, METRICS_PATH)


def machine_url(metrics_url: str) -> str:
    return urljoin(metrics_url, MACHINE_PATH)


def parse_machine(payload: dict[str, Any]) -> Machine:
    # Every field is required; an agent that doesn't send one is reported as malformed (ValidationError is a ValueError).
    return Machine.model_validate(payload)


class KafkaJsonProducer:
    """JSON records to one Kafka topic, keyed by machine id."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=to_json,
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    def _publish(self, records: Iterable[Metric | Machine]) -> None:
        queued = [self._producer.send(self._topic, value=r, key=r.machine_id) for r in records]
        for future in queued:
            future.get(timeout=SEND_TIMEOUT_SECONDS)

    def close(self) -> None:
        self._producer.flush(timeout=SEND_TIMEOUT_SECONDS)
        self._producer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class KafkaRawMetricsRepository(KafkaJsonProducer):
    @classmethod
    def from_settings(cls, kafka: KafkaSettings) -> KafkaRawMetricsRepository:
        return cls(bootstrap_servers=kafka.connection_string, topic=kafka.raw_metrics_topic)

    def publish(self, metrics: Iterable[Metric]) -> None:
        self._publish(metrics)


class KafkaMachinesRepository(KafkaJsonProducer):
    @classmethod
    def from_settings(cls, kafka: KafkaSettings) -> KafkaMachinesRepository:
        return cls(bootstrap_servers=kafka.connection_string, topic=kafka.machines_topic)

    def publish(self, machine: Machine) -> None:
        self._publish([machine])
