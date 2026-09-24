from dataclasses import dataclass


@dataclass
class RawMetrics:
    """What a client measures; sent by the collector to the raw metrics topic."""
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
    """RawMetrics enriched by the Flink job; consumed and written to TimescaleDB."""
    is_anomaly: bool
