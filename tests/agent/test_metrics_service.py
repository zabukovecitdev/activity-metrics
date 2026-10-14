from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from activityreporter.agent.services.metrics_service import MetricsService

service = MetricsService()


async def test_collect_metrics_returns_metrics_built_from_system_stats():
    fake_memory = MagicMock(total=100, used=50)

    with patch("activityreporter.agent.services.metrics_service.time.time", return_value=123.456), \
         patch("activityreporter.agent.services.metrics_service.psutil.boot_time", return_value=123.456 - 3600), \
         patch("activityreporter.agent.services.metrics_service.psutil.cpu_percent", return_value=15.0), \
         patch("activityreporter.agent.services.metrics_service.psutil.virtual_memory", return_value=fake_memory), \
         patch("activityreporter.agent.services.metrics_service.machineid.id", return_value="machine-123"):
        response = await service.collect_metrics()

    assert {m.name: m.value for m in response.metrics} == {
        "system.cpu.utilization": 15.0,
        "system.memory.usage": 50,
        "system.memory.limit": 100,
        "system.uptime": 3600.0,
    }
    assert (response.timestamp, response.machine_id) == (datetime.fromtimestamp(123.456, timezone.utc), "machine-123")


async def test_describe_machine_reports_static_info_and_when_it_was_observed():
    with patch("activityreporter.agent.services.metrics_service.time.time", return_value=1790595000.0), \
         patch("activityreporter.agent.services.metrics_service.psutil.boot_time", return_value=1790590000.0), \
         patch("activityreporter.agent.services.metrics_service.psutil.cpu_count", return_value=None), \
         patch("activityreporter.agent.services.metrics_service.machineid.id", return_value="machine-123"):
        machine = await service.describe_machine()

    assert machine.machine_id == "machine-123"
    assert machine.cores == 0
    assert machine.observed_at == 1790595000.0
    assert machine.last_boot == datetime.fromtimestamp(1790590000.0, timezone.utc)
    assert machine.total_memory > 0 and machine.total_disk > 0
