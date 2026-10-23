from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClickHouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")

    host: str = "localhost"
    port: int = 8123
    user: str = "user"
    password: SecretStr = SecretStr("password")
    db: str = "metrics"
    max_retries: int = Field(default=5, gt=0)
    backoff_base_seconds: float = Field(default=1.0, ge=0)
