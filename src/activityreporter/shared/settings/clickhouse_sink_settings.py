from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClickHouseSinkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WRITER_")

    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    poll_timeout_ms: int = Field(1000, gt=0)
