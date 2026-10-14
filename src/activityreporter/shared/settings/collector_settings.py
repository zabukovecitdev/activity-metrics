from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CollectorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="COLLECTOR_")

    scrape_interval_seconds: float = Field(1, gt=0)
    machine_refresh_seconds: float = Field(300, gt=0)
