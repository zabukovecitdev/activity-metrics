import asyncio
import logging
import signal
import sys

from prometheus_client import start_http_server

from activityreporter.collector.clients.agent_urls import AgentUrls
from activityreporter.collector.config.agents_file_loader import AgentsFileLoader
from activityreporter.collector.clients.http_agent_client import HttpAgentClient
from activityreporter.collector.producers.kafka_machines_producer import KafkaMachinesProducer
from activityreporter.collector.producers.kafka_metrics_producer import KafkaMetricsProducer
from activityreporter.collector.services.collector import Collector
from activityreporter.shared.observability.metrics import Metrics
from activityreporter.shared.settings.collector_settings import CollectorSettings
from activityreporter.shared.settings.kafka_settings import KafkaSettings

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
    endpoints = {AgentUrls.metrics_url(agent) for agent in agents}
    kafka = KafkaSettings()
    with (
        KafkaMetricsProducer.from_settings(kafka) as raw_metrics,
        KafkaMachinesProducer.from_settings(kafka) as machines,
    ):
        async with HttpAgentClient() as agent_metrics:
            collector = Collector(endpoints, agent_metrics, raw_metrics, machines, CollectorSettings())
            try:
                await collector.run()
            except asyncio.CancelledError:
                logger.info("SIGTERM received, shutting down")


def cli() -> None:
    try:
        agents = AgentsFileLoader.load(AGENTS_FILE)
    except (OSError, ValueError) as e:
        sys.exit(f"Cannot load agents: {e}")

    start_http_server(METRICS_PORT)
    try:
        asyncio.run(main(agents))
    except KeyboardInterrupt:
        Metrics.EXCEPTIONS.inc()


if __name__ == "__main__":
    cli()
