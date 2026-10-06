import asyncio
import logging
import signal
import sys

from activityreporter.collector.repository import (
    HttpAgentMetricsRepository,
    KafkaMachinesRepository,
    KafkaRawMetricsRepository,
    load_agents,
    metrics_url,
)
from activityreporter.collector.service import Collector

# Relative to the working directory: the repo root locally, /app in the container.
AGENTS_FILE = "collector.toml"


async def main(agents: list[str]) -> None:
    logging.basicConfig(level=logging.INFO)
    logging.info("Agents from %s: %s", AGENTS_FILE, agents)
    endpoints = {metrics_url(agent) for agent in agents}
    with KafkaRawMetricsRepository.from_env() as raw_metrics, KafkaMachinesRepository.from_env() as machines:
        async with HttpAgentMetricsRepository() as agent_metrics:
            collector = Collector(endpoints, agent_metrics, raw_metrics, machines)
            await collector.run()


def cli() -> None:
    try:
        agents = load_agents(AGENTS_FILE)
    except (OSError, ValueError) as e:
        sys.exit(f"Cannot load agents: {e}")

    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        asyncio.run(main(agents))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
