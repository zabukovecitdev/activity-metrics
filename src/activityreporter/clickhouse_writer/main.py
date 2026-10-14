import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from contextlib import ExitStack

from prometheus_client import start_http_server

from activityreporter.clickhouse_writer.consumers.kafka_records_consumer import KafkaRecordsConsumer
from activityreporter.clickhouse_writer.sinks.all_sinks import SINKS
from activityreporter.clickhouse_writer.repositories.clickhouse_repository import ClickHouseRepository
from activityreporter.shared.settings.clickhouse_settings import ClickHouseSettings
from activityreporter.shared.settings.clickhouse_sink_settings import ClickHouseSinkSettings
from activityreporter.shared.settings.kafka_settings import KafkaSettings

METRICS_PORT = 8000


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(METRICS_PORT)
    kafka, clickhouse, clickhouse_sink_settings = KafkaSettings(), ClickHouseSettings(), ClickHouseSinkSettings()
    with ExitStack() as stack:
        # A Kafka consumer and a ClickHouse client are not thread safe, so each sink gets its own pair.
        sinks = [
            sink_type.from_settings(
                stack.enter_context(
                    KafkaRecordsConsumer.from_settings(
                        kafka, clickhouse_sink_settings, sink_type.topic(kafka), sink_type.group_id()
                    )
                ),
                stack.enter_context(ClickHouseRepository.from_settings(clickhouse)),
                clickhouse_sink_settings,
            )
            for sink_type in SINKS
        ]

        def stop_all() -> None:
            for sink in sinks:
                sink.stop()

        signal.signal(signal.SIGTERM, lambda *_: stop_all())
        with ThreadPoolExecutor(len(sinks)) as pool:
            futures = [pool.submit(sink.run) for sink in sinks]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                # One sink failing stops the others, so the process exits and Compose restarts all three.
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
