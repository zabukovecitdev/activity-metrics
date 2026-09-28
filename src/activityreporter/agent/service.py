import time

from activityreporter.agent import repository
from activityreporter.agent.models import Machine
from activityreporter.shared.metrics import Metric


async def collect_metrics() -> list[Metric]:
    machine_id = repository.read_machine_id()
    cpu_utilization = repository.read_cpu_utilization()
    memory = repository.read_memory()
    battery = repository.read_battery()
    timestamp = time.time()

    def gauge(name: str, unit: str, value: float) -> Metric:
        return Metric(timestamp=timestamp, name=name, type="gauge", unit=unit, value=value, machine_id=machine_id)

    metrics = [
        gauge("system.cpu.utilization", "%", cpu_utilization),
        gauge("system.memory.usage", "By", memory.used),
        gauge("system.memory.limit", "By", memory.total),
    ]
    if battery is not None and battery.percent is not None:
        metrics.append(gauge("system.battery.utilization", "%", battery.percent))
    if battery is not None and battery.power_plugged is not None:
        metrics.append(gauge("system.battery.charging", "1", float(battery.power_plugged)))
    return metrics


async def describe_machine() -> Machine:
    return repository.read_machine()
