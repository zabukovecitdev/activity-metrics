from urllib.parse import urljoin

METRICS_PATH = "/v1/metrics"
MACHINE_PATH = "/v1/machine"


class AgentUrls:
    @staticmethod
    def metrics_url(agent_url: str) -> str:
        return urljoin(agent_url, METRICS_PATH)

    @staticmethod
    def machine_url(metrics_url: str) -> str:
        return urljoin(metrics_url, MACHINE_PATH)
