import os
import platform
import time
from datetime import datetime, timezone

import machineid
import psutil

from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric_observation import MetricObservation
from activityreporter.shared.models.metrics_response import MetricsResponse


class MetricsService:
    async def collect_metrics(self) -> MetricsResponse:
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
                gauge("system.cpu.utilization", "%", cpu_utilization),
                gauge("system.memory.usage", "By", memory.used),
                gauge("system.memory.limit", "By", memory.total),
                gauge("system.uptime", "s", uptime),
            ],
        )

    async def describe_machine(self) -> Machine:
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
