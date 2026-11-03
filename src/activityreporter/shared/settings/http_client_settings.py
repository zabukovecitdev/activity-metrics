from pydantic import Field
from pydantic_settings import BaseSettings


class HttpClientSettings(BaseSettings):
    """Default timeouts and connection limit for an HTTP client.

    A client has its own subclass with an `env_prefix`, so each client reads its own environment variables.
    """

    connect_timeout_seconds: float = Field(default=2, gt=0)
    read_timeout_seconds: float = Field(default=2, gt=0)
    max_connections: int = Field(default=200, gt=0)
