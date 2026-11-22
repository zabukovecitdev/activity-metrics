from datetime import datetime, timezone
from unittest.mock import patch

from activityreporter.agent.describe_machine.machine_describer import MachineDescriber

describer = MachineDescriber()


async def test_description_reports_static_info_and_when_it_was_observed():
    with patch("activityreporter.agent.describe_machine.machine_describer.time.time", return_value=1790595000.0), \
         patch("activityreporter.agent.describe_machine.machine_describer.psutil.boot_time", return_value=1790590000.0), \
         patch("activityreporter.agent.describe_machine.machine_describer.psutil.cpu_count", return_value=None), \
         patch("activityreporter.agent.describe_machine.machine_describer.machineid.id", return_value="machine-123"):
        machine = await describer.describe()

    assert machine.machine_id == "machine-123"
    assert machine.cores == 0
    assert machine.last_seen_at == 1790595000.0
    assert machine.last_boot == datetime.fromtimestamp(1790590000.0, timezone.utc)
    assert machine.total_memory > 0 and machine.total_disk > 0
