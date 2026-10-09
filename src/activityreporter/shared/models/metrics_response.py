from pydantic import AwareDatetime, BaseModel, Field

from activityreporter.shared.models.metric_observation import MetricObservation


class MetricsResponse(BaseModel):
    """What the agent serves on /v1/metrics: one snapshot, every metric sampled at `timestamp`."""
    machine_id: str = Field(min_length=1)
    timestamp: AwareDatetime
    metrics: list[MetricObservation]
