import time
from dataclasses import dataclass


@dataclass
class RawMetrics:
    timestamp: float | int
    cpu_usage: float
    memory_usage: float
    memory_total: float
    labels: dict
    machine_id: str
    battery_charging: bool | None
    battery_percentage: float | None


@dataclass
class ProcessedMetrics(RawMetrics):
    is_anomaly: bool

@dataclass
class Metric:
    timestamp: float | int
    name: str
    type: str
    unit: str
    value: float
    machine_id: str
    labels: dict
    attributes: dict[str, str] | None = None
    tenant_id: int | None = None  # ponytail: multi-tenancy not wired up yet, fill in when it is