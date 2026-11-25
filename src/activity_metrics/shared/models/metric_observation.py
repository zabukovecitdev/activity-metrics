from pydantic import BaseModel, Field


class MetricObservation(BaseModel):
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value: float
    attributes: dict[str, str] | None = None
