from dataclasses import dataclass


@dataclass
class MetricObservation:
    name: str
    type: str
    unit: str
    value: float
    attributes: dict[str, str] | None = None
