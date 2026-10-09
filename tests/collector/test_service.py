import uuid
from unittest.mock import patch

import httpx
import machineid
from kafka.errors import KafkaError
from prometheus_client import REGISTRY

from activityreporter.agent.main import app
from activityreporter.collector import service
from activityreporter.collector.repository import HttpAgentMetricsRepository, machine_url
from activityreporter.collector.service import Collector

ENDPOINT = "http://agent:8080/v1/metrics"


class RecordingRepository:
    def __init__(self, fail: bool = False):
        self.published = []
        self.dead_letters = []
        self._fail = fail

    def publish(self, records):
        if self._fail:
            raise KafkaError("broker down")
        self.published.extend(records if isinstance(records, list) else [records])

    def dead_letter(self, url, body, error):
        if self._fail:
            raise KafkaError("broker down")
        self.dead_letters.append((url, body, error))


def build_collector(agent_metrics, raw_metrics=None, machines=None) -> Collector:
    return Collector({ENDPOINT}, agent_metrics, raw_metrics or RecordingRepository(), machines or RecordingRepository())


async def collect_with_transport(transport: httpx.AsyncBaseTransport) -> list:
    raw_metrics = RecordingRepository()
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=transport)) as agent_metrics:
        await build_collector(agent_metrics, raw_metrics=raw_metrics).collect()
    return raw_metrics.published


async def test_each_agent_metric_becomes_its_own_message():
    published = await collect_with_transport(httpx.ASGITransport(app=app))

    assert "system.cpu.utilization" in {m.name for m in published}
    assert {m.machine_id for m in published} == {machineid.id()}
    assert all(m.type and m.unit and isinstance(m.timestamp, float) for m in published)


async def test_each_published_metric_gets_its_own_uuid7():
    published = await collect_with_transport(httpx.ASGITransport(app=app))

    ids = [uuid.UUID(m.metric_id) for m in published]
    assert {i.version for i in ids} == {7}
    assert len(set(ids)) == len(ids)


async def test_malformed_response_is_skipped():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"machine_id": "m1", "metrics": [{"name": "x"}]})

    assert await collect_with_transport(httpx.MockTransport(handler)) == []


async def test_metric_keeps_the_timestamp_it_was_sampled_at():
    with patch("activityreporter.agent.service.time.time", return_value=123.456):
        published = await collect_with_transport(httpx.ASGITransport(app=app))

    assert {m.timestamp for m in published} == {123.456}


def test_machine_url_is_next_to_the_metrics_url():
    assert machine_url("http://192.168.1.20:8080/v1/metrics") == "http://192.168.1.20:8080/v1/machine"


async def test_machine_info_is_published_on_first_scrape():
    machines = RecordingRepository()
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.ASGITransport(app=app))) as agent:
        await build_collector(agent, machines=machines).collect()

    assert len(machines.published) == 1
    machine = machines.published[0]
    assert machine.machine_id == machineid.id()
    assert machine.hostname
    assert isinstance(machine.observed_at, float)


async def test_machine_info_is_refreshed_only_after_the_interval():
    machines = RecordingRepository()
    clock = [1000.0]
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.ASGITransport(app=app))) as agent:
        collector = build_collector(agent, machines=machines)
        with patch("activityreporter.collector.service.time.monotonic", side_effect=lambda: clock[0]):
            await collector.collect()
            clock[0] += service.MACHINE_REFRESH_SECONDS - 1
            await collector.collect()
            clock[0] += 1
            await collector.collect()

    assert len(machines.published) == 2


async def test_failed_machine_publish_is_retried_on_next_scrape_and_metrics_still_flow():
    raw_metrics, failing = RecordingRepository(), RecordingRepository(fail=True)
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.ASGITransport(app=app))) as agent:
        collector = build_collector(agent, raw_metrics=raw_metrics, machines=failing)
        await collector.collect()

    assert raw_metrics.published
    assert collector._machine_refreshed_at == {}


async def test_agent_without_machine_endpoint_still_has_its_metrics_collected():
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.ASGITransport(app=app))) as real:
        payload = (await real._client.get(ENDPOINT)).json()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metrics":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    raw_metrics, machines = RecordingRepository(), RecordingRepository()
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.MockTransport(handler))) as agent:
        await build_collector(agent, raw_metrics=raw_metrics, machines=machines).collect()

    assert raw_metrics.published
    assert machines.published == []


def invalid_count(kind: str) -> float:
    return REGISTRY.get_sample_value("activityreporter_collector_invalid_responses_total", {"kind": kind}) or 0.0


async def collect_from(handler) -> tuple[RecordingRepository, RecordingRepository]:
    raw_metrics, machines = RecordingRepository(), RecordingRepository()
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=httpx.MockTransport(handler))) as agent:
        await build_collector(agent, raw_metrics=raw_metrics, machines=machines).collect()
    return raw_metrics, machines


async def test_invalid_metrics_and_machine_are_counted_and_dead_lettered_as_received():
    before = invalid_count("metrics"), invalid_count("machine")
    bodies = {"/v1/metrics": '{"machine_id": "m1", "metrics": [{"name": "x"}]}', "/v1/machine": "not json"}

    raw_metrics, machines = await collect_from(lambda request: httpx.Response(200, text=bodies[request.url.path]))

    assert (raw_metrics.published, machines.published) == ([], [])
    [(metrics_dlq_url, metrics_body, metrics_error)] = raw_metrics.dead_letters
    [(machine_dlq_url, machine_body, _)] = machines.dead_letters
    assert (metrics_dlq_url, metrics_body) == (ENDPOINT, bodies["/v1/metrics"])
    assert (machine_dlq_url, machine_body) == (machine_url(ENDPOINT), "not json")
    assert "validation error" in metrics_error
    assert (invalid_count("metrics"), invalid_count("machine")) == (before[0] + 1, before[1] + 1)


async def test_an_unreachable_agent_is_not_dead_lettered():
    raw_metrics, machines = await collect_from(lambda request: httpx.Response(503))

    assert (raw_metrics.dead_letters, machines.dead_letters) == ([], [])


async def test_a_failed_dead_letter_does_not_stop_the_scrape():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text="not json"))
    async with HttpAgentMetricsRepository(httpx.AsyncClient(transport=transport)) as agent:
        failing = RecordingRepository(fail=True)
        await build_collector(agent, raw_metrics=failing, machines=failing).collect()
