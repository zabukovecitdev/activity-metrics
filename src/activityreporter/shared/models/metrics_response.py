from dataclasses import dataclass

from activityreporter.shared.models.metric_observation import MetricObservation


@dataclass
class MetricsResponse:
    machine_id: str
    timestamp: str
    metrics: list[MetricObservation]
