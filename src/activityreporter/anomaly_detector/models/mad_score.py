from pydantic import BaseModel, ConfigDict


class MadScore(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float
    median: float
    # The scale the score is divided by, after the floor.
    scale: float
    # The scale from the values alone, before the floor.
    spread_scale: float = 0.0
