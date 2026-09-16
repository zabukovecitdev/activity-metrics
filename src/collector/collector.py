from asyncio import log

import requests

from client.metric_factory import Metrics
from connectors.kafka_connector import KafkaConnector


class Collector:
    def __init__(self, endpoints_urls:list[str],  connector: KafkaConnector):
        self.connector = connector
        self.endpoints = endpoints_urls

    async def collect(self) -> None:
        for endpoint in self.endpoints:
            try:
                response = requests.get(endpoint, timeout=5)
                response.raise_for_status()

                metrics = Metrics(**response.json())
                self.connector.send(metrics)
            except Exception as e:
                log.logger.error(e)