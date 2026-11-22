from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from activityreporter.agent.sample_metrics.metrics_sampler import MetricsSampler

sampler = MetricsSampler()


async def test_metrics_are_built_from_system_stats():
    fake_memory = MagicMock(total=100, used=50)

    with patch("activityreporter.agent.sample_metrics.metrics_sampler.time.time", return_value=123.456), \
         patch("activityreporter.agent.sample_metrics.metrics_sampler.psutil.boot_time", return_value=123.456 - 3600), \
         patch("activityreporter.agent.sample_metrics.metrics_sampler.psutil.cpu_percent", return_value=15.0), \
         patch("activityreporter.agent.sample_metrics.metrics_sampler.psutil.virtual_memory", return_value=fake_memory), \
         patch("activityreporter.agent.sample_metrics.metrics_sampler.machineid.id", return_value="machine-123"):
        response = await sampler.sample()

    assert {m.name: m.value for m in response.metrics} == {
        "system.cpu.utilization": 15.0,
        "system.memory.usage": 50,
        "system.memory.limit": 100,
        "system.uptime": 3600.0,
    }
    assert (response.timestamp, response.machine_id) == (datetime.fromtimestamp(123.456, timezone.utc), "machine-123")
