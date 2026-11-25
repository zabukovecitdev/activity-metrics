import uuid

from pydantic import BaseModel, Field, field_validator


class Metric(BaseModel):
    """One metric sample, as the collector publishes it on the raw metrics topic.

    `metric_id` (a UUIDv7 string) identifies the sample from the moment the
    collector accepts it. Unknown fields are ignored, so a producer may add one
    before every consumer knows it.
    """
    timestamp: float
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value: float
    machine_id: str = Field(min_length=1)
    metric_id: str
    attributes: dict[str, str] | None = None

    @field_validator("metric_id")
    @classmethod
    def metric_id_is_uuid(cls, value: str) -> str:
        uuid.UUID(value)
        return value
