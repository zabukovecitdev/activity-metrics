import time

from activityreporter.agent.readers.psutil_system_reader import PsutilSystemReader
from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric import Metric


class MetricsService:
    def __init__(self, reader: PsutilSystemReader):
        self._reader = reader

    async def collect_metrics(self) -> list[Metric]:
        machine_id = self._reader.read_machine_id()
        cpu_utilization = self._reader.read_cpu_utilization()
        memory = self._reader.read_memory()
        battery = self._reader.read_battery()
        uptime = self._reader.read_uptime()
        timestamp = time.time()

        def gauge(name: str, unit: str, value: float) -> Metric:
            return Metric(timestamp=timestamp, name=name, type="gauge", unit=unit, value=value, machine_id=machine_id)

        metrics = [
            gauge("system.cpu.utilization", "%", cpu_utilization),
            gauge("system.memory.usage", "By", memory.used),
            gauge("system.memory.limit", "By", memory.total),
            gauge("system.uptime", "s", uptime),
        ]
        if battery is not None and battery.percent is not None:
            metrics.append(gauge("system.battery.utilization", "%", battery.percent))
        if battery is not None and battery.power_plugged is not None:
            metrics.append(gauge("system.battery.charging", "1", float(battery.power_plugged)))
        return metrics

    async def describe_machine(self) -> Machine:
        return self._reader.read_machine()
