import time
import machineid
import psutil as util

from core.metrics import RawMetrics


def read_battery():
    # psutil raises instead of returning None when /sys/class/power_supply is
    # missing, e.g. inside a Docker container.
    try:
        return util.sensors_battery()
    except FileNotFoundError:
        return None


class MetricFactory:

    @staticmethod
    async def create_metrics(labels: dict) -> RawMetrics:
        cpu_usage: float = util.cpu_percent(interval=0.1)
        memory = util.virtual_memory()
        memory_total: int = memory.total
        memory_usage: int = memory.used
        machine_id: str = machineid.id()
        battery = read_battery()
        battery_charging: bool | None = battery.power_plugged if battery else None
        battery_percentage: float | None = battery.percent if battery else None

        return RawMetrics(timestamp=time.time(),
                          cpu_usage=cpu_usage,
                          memory_total=memory_total,
                          memory_usage=memory_usage,
                          labels=labels,
                          machine_id=machine_id,
                          battery_charging=battery_charging,
                          battery_percentage=battery_percentage)