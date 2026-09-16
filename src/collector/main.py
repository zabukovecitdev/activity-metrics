import asyncio

from collector.collector import Collector
from connectors.kafka_connector import KafkaConnector

endpoints = [
    "http://0.0.0.0:8080/metrics/"
]

async def main():
    with KafkaConnector.from_env() as connector:
        async with Collector(endpoints, connector) as collector:
            await collector.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
