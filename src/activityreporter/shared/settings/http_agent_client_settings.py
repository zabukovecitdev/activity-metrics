from pydantic_settings import SettingsConfigDict

from activityreporter.shared.settings.http_client_settings import HttpClientSettings


class HttpAgentClientSettings(HttpClientSettings):
    """Settings for the collector's HTTP client that fetches data from the agents."""

    model_config = SettingsConfigDict(env_prefix="AGENT_HTTP_")
