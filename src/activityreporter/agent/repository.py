import os
import platform
import time
from datetime import datetime, timezone

import machineid
import psutil

from activityreporter.agent.models import Machine


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


def read_machine() -> Machine:
    boot_time = psutil.boot_time()
    return Machine(
        machine_id=read_machine_id(),
        hostname=platform.node(),
        os=platform.system().lower(),
        os_version=platform.release(),
        architecture=platform.machine(),
        cores=psutil.cpu_count(),
        total_disk_memory=psutil.disk_usage(os.path.abspath(os.sep)).total,
        total_memory=psutil.virtual_memory().total,
        uptime=time.time() - boot_time,
        last_boot=datetime.fromtimestamp(boot_time, timezone.utc).isoformat(),
    )
