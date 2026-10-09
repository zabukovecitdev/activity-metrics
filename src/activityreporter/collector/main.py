import asyncio
import logging
import signal
import sys

from prometheus_client import Counter, start_http_server

from activityreporter.collector.repository import (
    HttpAgentMetricsRepository,
    KafkaMachinesRepository,
    KafkaRawMetricsRepository,
    load_agents,
    metrics_url,
)
from activityreporter.collector.service import Collector
from activityreporter.shared.settings import KafkaSettings

EXCEPTIONS = Counter(
    "activityreporter_collector_exceptions_total",
    "Total exceptions raised in the collector",
)
# Relative to the working directory: the repo root locally, /app in the container.
AGENTS_FILE = "collector.toml"
METRICS_PORT = 8000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(agents: list[str]) -> None:
    logger.info("Agents from %s: %s", AGENTS_FILE, agents)

    main_task = asyncio.current_task()
    assert main_task is not None
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, main_task.cancel)
    endpoints = {metrics_url(agent) for agent in agents}
    kafka = KafkaSettings()
    with (
        KafkaRawMetricsRepository.from_settings(kafka) as raw_metrics,
        KafkaMachinesRepository.from_settings(kafka) as machines,
    ):
        async with HttpAgentMetricsRepository() as agent_metrics:
            collector = Collector(endpoints, agent_metrics, raw_metrics, machines)
            try:
                await collector.run()
            except asyncio.CancelledError:
                logger.info("SIGTERM received, shutting down")


def cli() -> None:
    try:
        agents = load_agents(AGENTS_FILE)
    except (OSError, ValueError) as e:
        sys.exit(f"Cannot load agents: {e}")

    start_http_server(METRICS_PORT)
    try:
        asyncio.run(main(agents))
    except KeyboardInterrupt:
        EXCEPTIONS.inc()


if __name__ == "__main__":
    cli()
