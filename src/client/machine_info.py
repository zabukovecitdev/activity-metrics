import os
import platform
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import machineid
import psutil as util

from client.metric_factory import read_battery


@dataclass
class MachineInfo():
    machine_id: str
    machine_name: str
    os_name: str
    battery_charging: bool | None
    battery_percentage: float | None
    cores: int
    total_disk_memory: float
    total_memory: float
    uptime: float
    last_boot: str


class MachineInfoFactory:

    @staticmethod
    async def create_machine_info() -> MachineInfo:
        battery = read_battery()
        boot_time: float = util.boot_time()

        return MachineInfo(machine_id=machineid.id(),
                           machine_name=platform.node(),
                           os_name=f"{platform.system()} {platform.release()}",
                           battery_charging=battery.power_plugged if battery else None,
                           battery_percentage=battery.percent if battery else None,
                           cores=util.cpu_count(),
                           total_disk_memory=util.disk_usage(os.path.abspath(os.sep)).total,
                           total_memory=util.virtual_memory().total,
                           uptime=time.time() - boot_time,
                           last_boot=datetime.fromtimestamp(boot_time, timezone.utc).isoformat())
