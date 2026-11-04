import os
import platform
import time
from datetime import datetime, timezone

import machineid
from activityreporter.shared.models.units import Units
import psutil

from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric_observation import MetricObservation
from activityreporter.shared.models.metrics_response import MetricsResponse


class MetricsService:
    async def collect_metrics(self) -> MetricsResponse:
        """Reads the CPU, memory and uptime of this machine.

        Returns:
            The metrics of this machine, with the current time.
        """
        machine_id = machineid.id()
        cpu_utilization = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        uptime = time.time() - psutil.boot_time()
        timestamp = time.time()

        def gauge(name: str, unit: str, value: float) -> MetricObservation:
            return MetricObservation(name=name, type="gauge", unit=unit, value=value)

        return MetricsResponse(
            machine_id=machine_id,
            timestamp=datetime.fromtimestamp(timestamp, timezone.utc),
            metrics=[
                gauge("system.cpu.utilization", Units.PERCENT.value, cpu_utilization),
                gauge("system.memory.usage", Units.BYTES.value, memory.used),
                gauge("system.memory.limit", Units.BYTES.value, memory.total),
                gauge("system.uptime", Units.SECONDS.value, uptime),
            ],
        )

    async def describe_machine(self) -> Machine:
        """Reads the hardware and operating system facts of this machine.

        Returns:
            The description of this machine.
        """
        return Machine(
            machine_id=machineid.id(),
            hostname=platform.node(),
            os=platform.system().lower(),
            os_version=platform.release(),
            architecture=platform.machine(),
            cores=psutil.cpu_count() or 0,
            total_memory=psutil.virtual_memory().total,
            total_disk=psutil.disk_usage(os.path.abspath(os.sep)).total,
            last_boot=datetime.fromtimestamp(psutil.boot_time(), timezone.utc),
            last_seen_at=time.time(),
        )
