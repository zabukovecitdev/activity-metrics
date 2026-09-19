from unittest.mock import MagicMock, patch

from client.metric_factory import MetricFactory


async def test_create_metrics_returns_metrics_built_from_system_stats():
    fake_memory = MagicMock(total=100, used=50)

    with patch("client.metric_factory.time.time", return_value=123.456), \
         patch("client.metric_factory.util.cpu_percent", return_value=15.0), \
         patch("client.metric_factory.util.virtual_memory", return_value=fake_memory), \
         patch("client.metric_factory.machineid.id", return_value="machine-123"):
        metrics = await MetricFactory.create_metrics({"name": "test"})

    assert metrics.timestamp == 123.456
    assert metrics.cpu_usage == 15.0
    assert metrics.memory_total == 100
    assert metrics.memory_usage == 50
    assert metrics.labels == {"name": "test"}
    assert metrics.machine_id == "machine-123"
