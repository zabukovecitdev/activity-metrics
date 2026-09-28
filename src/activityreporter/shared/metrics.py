from dataclasses import dataclass


@dataclass
class Metric:
    timestamp: float | int
    name: str
    type: str
    unit: str
    value: float
    machine_id: str
    attributes: dict[str, str] | None = None


@dataclass
class ProcessedMetric(Metric):
    is_anomaly: bool = False
