import os
import platform
import time
from datetime import datetime, timezone

import machineid
import psutil

from activityreporter.contracts.machine import Machine


class MachineDescriber:
    async def describe(self) -> Machine:
        return Machine(
            machine_id=machineid.id(),
            hostname=platform.node(),
            os=platform.system().lower(),
            os_version=platform.release(),
            architecture=platform.machine(),
            # cpu_count() is None when the platform can't tell.
            cores=psutil.cpu_count() or 0,
            total_memory=psutil.virtual_memory().total,
            total_disk=psutil.disk_usage(os.path.abspath(os.sep)).total,
            last_boot=datetime.fromtimestamp(psutil.boot_time(), timezone.utc),
            last_seen_at=time.time(),
        )
