import time
import machineid
import psutil as util

from core.metrics import Metric


def read_battery():
    # psutil raises instead of returning None when /sys/class/power_supply is
    # missing, e.g. inside a Docker container.
    try:
        return util.sensors_battery()
    except FileNotFoundError:
        return None


class MetricFactory:

    @staticmethod
    async def create_metrics(labels: dict) -> list[Metric]:
        cpu_usage: float = util.cpu_percent(interval=0.1)
        memory = util.virtual_memory()
        memory_total: int = memory.total
        memory_usage: int = memory.used
        machine_id: str = machineid.id()
        battery = read_battery()
        battery_charging: bool | None = battery.power_plugged if battery else None
        battery_percentage: float | None = battery.percent if battery else None
        timestamp = time.time()

        metrics = [
            Metric(timestamp=timestamp, name="cpu_usage", value=cpu_usage, machine_id=machine_id, labels=labels),
            Metric(timestamp=timestamp, name="memory_usage", value=memory_usage, machine_id=machine_id, labels=labels),
            Metric(timestamp=timestamp, name="memory_total", value=memory_total, machine_id=machine_id, labels=labels),
        ]
        if battery_percentage is not None:
            metrics.append(Metric(timestamp=timestamp, name="battery_percentage", value=battery_percentage,
                                   machine_id=machine_id, labels=labels))
        if battery_charging is not None:
            metrics.append(Metric(timestamp=timestamp, name="battery_charging", value=float(battery_charging),
                                   machine_id=machine_id, labels=labels))

        return metrics