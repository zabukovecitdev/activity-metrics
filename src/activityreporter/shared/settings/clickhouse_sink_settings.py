from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class ClickHouseSinkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WRITER_")

    batch_size: int = DEFAULT_BATCH_SIZE
    batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS
    poll_timeout_ms: int = Field(1000, gt=0)
