import time
from datetime import datetime, timezone

import machineid
import psutil

from activityreporter.shared.models.metric_observation import MetricObservation
from activityreporter.shared.models.metrics_response import MetricsResponse


class MetricsSampler:
    async def sample(self) -> MetricsResponse:
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
