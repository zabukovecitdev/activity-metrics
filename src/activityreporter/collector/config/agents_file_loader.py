import tomllib
from urllib.parse import urlparse


class AgentsFileLoader:
    @staticmethod
    def load(path: str) -> list[str]:
        """Base URLs of the agents to scrape, from `agents` in the TOML file at `path`.

        Raises OSError if the file can't be read and ValueError, naming the problem, if it isn't a non-empty list of
        http(s) URLs.
        """
        try:
            with open(path, "rb") as f:
                config = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"{path}: {e}") from e

        agents = config.get("agents")
        if not isinstance(agents, list) or not agents:
            raise ValueError(f"{path}: agents must be a non-empty list of URLs")
        for i, agent in enumerate(agents):
            parsed = urlparse(agent) if isinstance(agent, str) else None
            if parsed is None or parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise ValueError(f"{path}: agents[{i}] {agent!r} must start with http:// or https://")
        return agents
