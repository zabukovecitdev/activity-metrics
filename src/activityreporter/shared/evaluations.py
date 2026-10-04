from dataclasses import dataclass


@dataclass
class Evaluation:
    """One metric sample scored by one algorithm, as published on the evaluations topic.

    Every scored sample gets one per algorithm, anomalous or not, so the line and
    band can be drawn next to the series. `metric_id` is the sample's id in the
    metrics table, which holds its value. `lower` is None for an algorithm
    without a lower band.
    """
    metric_id: str
    machine_id: str
    metric_name: str
    timestamp: float
    algorithm: str
    baseline: float
    lower: float | None
    upper: float
    is_anomaly: bool
