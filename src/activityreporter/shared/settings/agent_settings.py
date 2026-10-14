from pydantic_settings import BaseSettings


class AgentSettings(BaseSettings):
    port: int = 8080
