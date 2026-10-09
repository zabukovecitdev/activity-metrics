"""Every environment variable the applications read, parsed and defaulted in one place."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT_SECONDS = 5.0


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    connection_string: str = "localhost:9094"
    raw_metrics_topic: str = "raw_metrics"
    evaluations_topic: str = "evaluations"
    machines_topic: str = "machines"
    connector_jar: str = ""  # The Flink Kafka connector jar; empty when the image already has it on the classpath.


class ClickHouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")

    host: str = "localhost"
    port: int = 8123
    user: str = "user"
    password: SecretStr = SecretStr("password")
    db: str = "metrics"


class WriterSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WRITER_")

    batch_size: int = DEFAULT_BATCH_SIZE
    batch_timeout_seconds: float = DEFAULT_BATCH_TIMEOUT_SECONDS


class AgentSettings(BaseSettings):
    port: int = 8080
