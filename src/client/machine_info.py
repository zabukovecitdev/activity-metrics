import os
import platform
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import machineid
import psutil as util


@dataclass
class Machine():
    machine_id: str
    hostname: str
    os: str
    os_version: str
    architecture: str
    cores: int
    total_disk_memory: float
    total_memory: float
    uptime: float
    last_boot: str


class MachineFactory:

    @staticmethod
    async def create_machine() -> Machine:
        boot_time: float = util.boot_time()

        return Machine(machine_id=machineid.id(),
                        hostname=platform.node(),
                        os=platform.system().lower(),
                        os_version=platform.release(),
                        architecture=platform.machine(),
                        cores=util.cpu_count(),
                        total_disk_memory=util.disk_usage(os.path.abspath(os.sep)).total,
                        total_memory=util.virtual_memory().total,
                        uptime=time.time() - boot_time,
                        last_boot=datetime.fromtimestamp(boot_time, timezone.utc).isoformat())
