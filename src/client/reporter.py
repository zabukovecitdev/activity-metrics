from abc import ABC

from client.metric_factory import MetricFactory
from connectors.kafka_connector import KafkaConnector
from core.metrics import RawMetrics


class BaseReporter(ABC):
    async def get(self) -> RawMetrics:
        raise NotImplementedError()

    async def report(self) -> None:
        raise NotImplementedError()

class ConsoleReporter(BaseReporter):
    async def report(self) -> None:
        print(await MetricFactory.create_metrics({"name": "ConsoleReporter"}))

class KafkaReporter(BaseReporter):
    def __init__(self, connector: KafkaConnector):
        self._connector = connector
    async def report(self) -> None:
        self._connector.send(await MetricFactory.create_metrics({"name": "KafkaReporter"}))

class HttpReporter(BaseReporter):
    async def get(self) -> RawMetrics:
        return await MetricFactory.create_metrics({"name": "HttpReporter"})