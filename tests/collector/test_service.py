import httpx
import machineid

from activityreporter.agent.main import app
from activityreporter.collector.service import Collector


class RecordingConnector:
    def __init__(self):
        self.sent = []

    def send(self, metrics):
        self.sent.extend(metrics)


async def collect_with_transport(transport: httpx.AsyncBaseTransport) -> list:
    connector = RecordingConnector()
    async with Collector(lambda: {"http://client:8080/v1/metrics"}, connector) as collector:
        await collector._client.aclose()
        collector._client = httpx.AsyncClient(transport=transport)
        await collector.collect()
    return connector.sent


async def test_each_client_metric_becomes_its_own_message():
    sent = await collect_with_transport(httpx.ASGITransport(app=app))

    assert "system.cpu.utilization" in {m.name for m in sent}
    assert {m.machine_id for m in sent} == {machineid.id()}
    assert all(m.type and m.unit and isinstance(m.timestamp, float) for m in sent)


async def test_malformed_response_is_skipped():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"machine_id": "m1", "metrics": [{"name": "x"}]})

    assert await collect_with_transport(httpx.MockTransport(handler)) == []
