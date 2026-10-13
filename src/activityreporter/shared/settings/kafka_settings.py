from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    connection_string: str = "localhost:9094"
    raw_metrics_topic: str = "raw_metrics"
    evaluations_topic: str = "evaluations"
    machines_topic: str = "machines"
    connector_jar: str = ""  # The Flink Kafka connector jar; empty when the image already has it on the classpath.
