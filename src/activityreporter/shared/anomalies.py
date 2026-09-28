from dataclasses import dataclass


@dataclass
class Anomaly:
    """One scored metric sample, as published on the anomalies topic.

    `machine_id`, `metric_name`, `timestamp` and `metric_id` are the key of
    the sample in the metrics table. Algorithm-specific numbers go in
    `details`, so a new algorithm doesn't change the schema.
    """
    metric_id: str
    machine_id: str
    metric_name: str
    timestamp: float
    value: float
    algorithm: str
    score: float
    threshold: float
    direction: str
    detected_at: float
    metric_attributes: dict[str, str] | None = None
    details: dict[str, float] | None = None
    schema_version: int = 1
