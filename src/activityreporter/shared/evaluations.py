from dataclasses import dataclass


@dataclass
class Evaluation:
    """One metric sample scored by one algorithm, as published on the evaluations topic.

    Every scored sample gets one per algorithm, anomalous or not, so the
    baseline and bands can be drawn next to the raw series. `metric_id` is the
    sample's id in the metrics table. `baseline`, `lower`, `upper`, `score`,
    `threshold`, `is_anomaly` and `direction` are the fields every algorithm
    fills; `params` holds its inputs and `details` what it computed, so a new
    algorithm doesn't change the schema. A band the algorithm doesn't have is None.
    """
    metric_id: str
    machine_id: str
    metric_name: str
    timestamp: float
    value: float
    algorithm: str
    algorithm_version: int
    params: dict[str, float]
    baseline: float | None
    lower: float | None
    upper: float | None
    score: float
    threshold: float
    is_anomaly: bool
    direction: int
    details: dict[str, float]
    detected_at: float
    schema_version: int = 1
