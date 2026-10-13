import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from contextlib import ExitStack

from prometheus_client import start_http_server

from activityreporter.clickhouse_writer.consumers.kafka_records_consumer import KafkaRecordsConsumer
from activityreporter.clickhouse_writer.ingesters.all_ingesters import INGESTERS
from activityreporter.clickhouse_writer.repositories.clickhouse_repository import ClickHouseRepository
from activityreporter.shared.settings.clickhouse_settings import ClickHouseSettings
from activityreporter.shared.settings.ingester_settings import IngesterSettings
from activityreporter.shared.settings.kafka_settings import KafkaSettings

METRICS_PORT = 8000


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(METRICS_PORT)
    kafka, clickhouse, ingester_settings = KafkaSettings(), ClickHouseSettings(), IngesterSettings()
    with ExitStack() as stack:
        # A Kafka consumer and a ClickHouse client are not thread safe, so each ingester gets its own pair.
        ingesters = [
            ingester_type.from_settings(
                stack.enter_context(
                    KafkaRecordsConsumer.from_settings(kafka, ingester_type.topic(kafka), ingester_type.group_id())
                ),
                stack.enter_context(ClickHouseRepository.from_settings(clickhouse)),
                ingester_settings,
            )
            for ingester_type in INGESTERS
        ]

        def stop_all() -> None:
            for ingester in ingesters:
                ingester.stop()

        signal.signal(signal.SIGTERM, lambda *_: stop_all())
        with ThreadPoolExecutor(len(ingesters)) as pool:
            futures = [pool.submit(ingester.run) for ingester in ingesters]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                # One ingester failing stops the others, so the process exits and Compose restarts all three.
                stop_all()
        for future in futures:
            future.result()


def cli() -> None:
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
