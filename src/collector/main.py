import asyncio
import os

from collector.collector import Collector
from connectors.kafka_connector import KafkaConnector

DEFAULT_ENDPOINTS = "http://localhost:8080/metrics/"


async def main() -> None:
    endpoints = os.environ.get("COLLECTOR_ENDPOINTS", DEFAULT_ENDPOINTS).split(",")
    with KafkaConnector.from_env() as connector:
        async with Collector(endpoints, connector) as collector:
            await collector.run()


def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
