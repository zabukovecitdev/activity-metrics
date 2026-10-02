from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from activityreporter.agent import service


async def test_collect_metrics_returns_metrics_built_from_system_stats():
    fake_memory = MagicMock(total=100, used=50)

    with patch("activityreporter.agent.service.time.time", return_value=123.456), \
         patch("activityreporter.agent.repository.psutil.cpu_percent", return_value=15.0), \
         patch("activityreporter.agent.repository.psutil.virtual_memory", return_value=fake_memory), \
         patch("activityreporter.agent.repository.machineid.id", return_value="machine-123"), \
         patch("activityreporter.agent.repository.read_battery", return_value=None), \
         patch("activityreporter.agent.repository.read_uptime", return_value=3600.0):
        metrics = await service.collect_metrics()

    assert {m.name: m.value for m in metrics} == {
        "system.cpu.utilization": 15.0,
        "system.memory.usage": 50,
        "system.memory.limit": 100,
        "system.uptime": 3600.0,
    }
    assert {(m.timestamp, m.machine_id) for m in metrics} == {(123.456, "machine-123")}


async def test_collect_metrics_reports_no_battery_when_power_supply_is_missing():
    with patch("activityreporter.agent.repository.psutil.sensors_battery",
               side_effect=FileNotFoundError("/sys/class/power_supply")):
        metrics = await service.collect_metrics()

    assert not any(m.name.startswith("system.battery.") for m in metrics)


async def test_describe_machine_reports_static_info_and_when_it_was_observed():
    with patch("activityreporter.agent.repository.time.time", return_value=1790595000.0), \
         patch("activityreporter.agent.repository.psutil.boot_time", return_value=1790590000.0), \
         patch("activityreporter.agent.repository.psutil.cpu_count", return_value=None), \
         patch("activityreporter.agent.repository.machineid.id", return_value="machine-123"):
        machine = await service.describe_machine()

    assert machine.machine_id == "machine-123"
    assert machine.cores == 0
    assert machine.observed_at == 1790595000.0
    assert datetime.fromisoformat(machine.last_boot) == datetime.fromtimestamp(1790590000.0, timezone.utc)
    assert machine.total_memory > 0 and machine.total_disk > 0
