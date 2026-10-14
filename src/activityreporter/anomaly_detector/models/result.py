from pydantic import BaseModel, ConfigDict


class Result(BaseModel):
    """What every algorithm reports for the latest value of a window, in the value's units:
    the line and band to draw, and whether the value is outside it."""
    model_config = ConfigDict(frozen=True)

    baseline: float
    lower: float | None
    upper: float
    is_anomaly: bool
