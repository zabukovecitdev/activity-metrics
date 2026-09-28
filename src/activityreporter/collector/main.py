import asyncio
import logging
import os
import signal

from activityreporter.collector.discovery import ServiceDiscovery
from activityreporter.collector.repository import HttpAgentMetricsRepository, KafkaRawMetricsRepository
from activityreporter.collector.service import Collector


def static_endpoints(raw: str) -> set[str]:
    return {url.strip() for url in raw.split(",") if url.strip()}


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    endpoints = static_endpoints(os.environ.get("COLLECTOR_ENDPOINTS", ""))
    logging.info("Static endpoints: %s", sorted(endpoints) or "none")
    with KafkaRawMetricsRepository.from_env() as raw_metrics:
        async with ServiceDiscovery() as discovery, HttpAgentMetricsRepository() as agent_metrics:
            collector = Collector(lambda: endpoints | discovery.urls(), agent_metrics, raw_metrics)
            await collector.run()


def cli() -> None:
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
