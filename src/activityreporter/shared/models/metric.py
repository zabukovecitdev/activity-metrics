from pydantic import BaseModel, Field


class Metric(BaseModel):
    """One metric sample.

    `metric_id` (a UUIDv7 string) identifies the sample from the moment the
    collector accepts it; the agent builds samples without one. Unknown fields
    are ignored, so a producer may add one before every consumer knows it.
    """
    timestamp: float
    name: str
    type: str
    unit: str
    value: float
    machine_id: str = Field(min_length=1)
    metric_id: str | None = None
    attributes: dict[str, str] | None = None
