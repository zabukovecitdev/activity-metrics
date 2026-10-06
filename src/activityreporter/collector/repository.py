from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterable
from dataclasses import asdict, fields
from datetime import datetime
from typing import Any, Self
from urllib.parse import urljoin

import httpx
from kafka import KafkaProducer

from activityreporter.shared.models import Machine, Metric

CONNECT_TIMEOUT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2
MAX_CONNECTIONS = 200
SEND_TIMEOUT_SECONDS = 10
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
            metric_id=str(uuid.uuid7()),
            attributes=m.get("attributes"),
        )
        for m in payload["metrics"]
    ]


def machine_url(metrics_url: str) -> str:
    return urljoin(metrics_url, MACHINE_PATH)


def parse_machine(payload: dict[str, Any]) -> Machine:
    # Every field is required; an agent that doesn't send one is reported as malformed.
    machine = Machine(**{f.name: payload[f.name] for f in fields(Machine)})
    if not machine.machine_id:
        raise ValueError("machine_id is empty")
    return machine


class KafkaJsonProducer:
    """JSON records to one Kafka topic, keyed by machine id."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    def _publish(self, records: Iterable[Metric | Machine]) -> None:
        queued = [self._producer.send(self._topic, value=asdict(r), key=r.machine_id) for r in records]
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
    def from_env(cls) -> KafkaRawMetricsRepository:
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            topic=os.environ.get("KAFKA_RAW_METRICS_TOPIC", "raw_metrics"),
        )

    def publish(self, metrics: Iterable[Metric]) -> None:
        self._publish(metrics)


class KafkaMachinesRepository(KafkaJsonProducer):
    @classmethod
    def from_env(cls) -> KafkaMachinesRepository:
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094"),
            topic=os.environ.get("KAFKA_MACHINES_TOPIC", "machines"),
        )

    def publish(self, machine: Machine) -> None:
        self._publish([machine])
