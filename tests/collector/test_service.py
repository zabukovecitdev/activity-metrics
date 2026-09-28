from unittest.mock import patch

import httpx
import machineid

from activityreporter.agent.main import app
from activityreporter.collector.repository import HttpAgentMetricsRepository
from activityreporter.collector.service import Collector


class RecordingRawMetricsRepository:
    def __init__(self):
        self.published = []

    def publish(self, metrics):
        self.published.extend(metrics)


async def collect_with_transport(transport: httpx.AsyncBaseTransport) -> list:
    raw_metrics = RecordingRawMetricsRepository()
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=transport)) as agent_metrics:
        await Collector(lambda: {"http://agent:8080/v1/metrics"}, agent_metrics, raw_metrics).collect()
    return raw_metrics.published


async def test_each_agent_metric_becomes_its_own_message():
    published = await collect_with_transport(httpx.ASGITransport(app=app))

    assert "system.cpu.utilization" in {m.name for m in published}
    assert {m.machine_id for m in published} == {machineid.id()}
    assert all(m.type and m.unit and isinstance(m.timestamp, float) for m in published)


async def test_malformed_response_is_skipped():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"machine_id": "m1", "metrics": [{"name": "x"}]})

    assert await collect_with_transport(httpx.MockTransport(handler)) == []


async def test_metric_keeps_the_timestamp_it_was_sampled_at():
    with patch("activityreporter.agent.service.time.time", return_value=123.456):
        published = await collect_with_transport(httpx.ASGITransport(app=app))

    assert {m.timestamp for m in published} == {123.456}
