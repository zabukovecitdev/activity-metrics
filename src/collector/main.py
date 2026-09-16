import asyncio

from collector.collector import Collector
from connectors.kafka_connector import KafkaConnector

endpoints = [
    "http://0.0.0.0:8080"
]

async def main():
    with KafkaConnector.from_env() as connector:
        collector = Collector(endpoints, connector)
        while True:
            await collector.collect()
            await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass