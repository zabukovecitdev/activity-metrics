import uuid

from pydantic import BaseModel, Field, field_validator


class Evaluation(BaseModel):
    """One metric sample scored by one algorithm, as published on the evaluations topic.

    Every scored sample gets one per algorithm, anomalous or not, so the line and
    band can be drawn next to the series. `metric_id` is the sample's id in the
    metrics table, which holds its value. `lower` is None for an algorithm
    without a lower band.
    """
    metric_id: str
    machine_id: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    timestamp: float
    algorithm: str = Field(min_length=1)
    baseline: float
    lower: float | None
    upper: float
    is_anomaly: bool

    @field_validator("metric_id")
    @classmethod
    def metric_id_is_uuid(cls, value: str) -> str:
        uuid.UUID(value)
        return value
