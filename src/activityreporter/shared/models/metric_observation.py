from pydantic import BaseModel


class MetricObservation(BaseModel):
    name: str
    type: str
    unit: str
    value: float
    attributes: dict[str, str] | None = None
