import time
from dataclasses import dataclass
import machineid
import psutil as util

@dataclass
class Metrics:
    timestamp: float|int
    boot_time: float
    core_count: int | None
    cpu_usage: list[float]
    memory_usage: float
    memory_total: float
    labels: dict
    machine_id: str

class MetricFactory:

    @staticmethod
    async def create_metrics(labels: dict) -> Metrics:
        boot_time: float = util.boot_time()
        cpu_count: int | None = util.cpu_count()
        cpu_usage: list[float | int] = util.cpu_percent(interval=0.1, percpu=True)
        memory = util.virtual_memory()
        memory_total: int = memory.total
        memory_usage: int = memory.used
        machine_id: str = machineid.id()

        return Metrics(timestamp=time.time(),
                       boot_time=boot_time,
                       core_count=cpu_count,
                       cpu_usage=cpu_usage,
                       memory_total=memory_total,
                       memory_usage=memory_usage,
                       labels=labels,
                       machine_id=machine_id)