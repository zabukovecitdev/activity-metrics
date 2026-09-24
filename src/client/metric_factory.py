import time
from dataclasses import dataclass
import machineid
import psutil as util

@dataclass
class Metrics:
    timestamp: float|int
    cpu_usage: float
    memory_usage: float
    memory_total: float
    labels: dict
    machine_id: str
    battery_charging: bool | None
    battery_percentage: float | None

class MetricFactory:

    @staticmethod
    async def create_metrics(labels: dict) -> Metrics:
        cpu_usage: float = util.cpu_percent(interval=0.1)
        memory = util.virtual_memory()
        memory_total: int = memory.total
        memory_usage: int = memory.used
        machine_id: str = machineid.id()
        battery_charging: bool | None = util.sensors_battery().power_plugged if util.sensors_battery() else None
        battery_percentage: float | None = util.sensors_battery().percent if util.sensors_battery() else None

        return Metrics(timestamp=time.time(),
                       cpu_usage=cpu_usage,
                       memory_total=memory_total,
                       memory_usage=memory_usage,
                       labels=labels,
                       machine_id=machine_id,
                       battery_charging=battery_charging,
                       battery_percentage=battery_percentage)