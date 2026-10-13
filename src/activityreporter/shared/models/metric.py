import uuid

from pydantic import BaseModel, Field, field_validator


class Metric(BaseModel):
    """One metric sample.

    `metric_id` (a UUIDv7 string) identifies the sample from the moment the
    collector accepts it; the agent builds samples without one. Unknown fields
    are ignored, so a producer may add one before every consumer knows it.
    """
    timestamp: float
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value: float
    machine_id: str = Field(min_length=1)
    metric_id: str | None = None
    attributes: dict[str, str] | None = None

    @field_validator("metric_id")
    @classmethod
    def metric_id_is_uuid(cls, value: str | None) -> str | None:
        if value is not None:
            uuid.UUID(value)
        return value
