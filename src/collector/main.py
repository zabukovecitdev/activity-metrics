import asyncio
import logging
import os

from collector.collector import Collector
from collector.discovery import ServiceDiscovery
from connectors.kafka_connector import KafkaConnector


def static_endpoints(raw: str) -> set[str]:
    return {url.strip() for url in raw.split(",") if url.strip()}


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # Clients found over mDNS are scraped together with COLLECTOR_ENDPOINTS, which
    # covers clients mDNS can't reach, e.g. across a Docker bridge network.
    endpoints = static_endpoints(os.environ.get("COLLECTOR_ENDPOINTS", ""))
    logging.info("Static endpoints: %s", sorted(endpoints) or "none")
    with KafkaConnector.from_env() as connector:
        async with ServiceDiscovery() as discovery:
            async with Collector(lambda: endpoints | discovery.urls(), connector) as collector:
                await collector.run()


def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
