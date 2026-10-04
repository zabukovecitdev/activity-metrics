import os
import platform
import time
from datetime import datetime, timezone

import machineid
import psutil

from activityreporter.shared.models import Machine


def read_machine_id() -> str:
    return machineid.id()


def read_cpu_utilization() -> float:
    return psutil.cpu_percent(interval=0.1)


def read_memory():
    return psutil.virtual_memory()


def read_battery():
    try:
        return psutil.sensors_battery()
    except FileNotFoundError:
        return None


def read_uptime() -> float:
    return time.time() - psutil.boot_time()


def read_machine() -> Machine:
    return Machine(
        machine_id=read_machine_id(),
        hostname=platform.node(),
        os=platform.system().lower(),
        os_version=platform.release(),
        architecture=platform.machine(),
        # cpu_count() is None when the platform can't tell.
        cores=psutil.cpu_count() or 0,
        total_memory=psutil.virtual_memory().total,
        total_disk=psutil.disk_usage(os.path.abspath(os.sep)).total,
        last_boot=datetime.fromtimestamp(psutil.boot_time(), timezone.utc).isoformat(),
        observed_at=time.time(),
    )
