from pydantic import BaseModel, ConfigDict


class EwmaScore(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float
    average: float
    scale: float
