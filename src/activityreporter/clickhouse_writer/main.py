import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from contextlib import ExitStack

from prometheus_client import start_http_server

from activityreporter.clickhouse_writer.repository import ClickHouseRepository, KafkaRecordsRepository
from activityreporter.clickhouse_writer.writers import WRITERS
from activityreporter.shared.settings import ClickHouseSettings, KafkaSettings, WriterSettings

METRICS_PORT = 8000


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(METRICS_PORT)
    kafka, clickhouse, writer_settings = KafkaSettings(), ClickHouseSettings(), WriterSettings()
    with ExitStack() as stack:
        # A Kafka consumer and a ClickHouse client are not thread safe, so each writer gets its own pair.
        writers = [
            writer_type.from_settings(
                stack.enter_context(
                    KafkaRecordsRepository.from_settings(kafka, writer_type.topic(kafka), writer_type.group_id())
                ),
                stack.enter_context(ClickHouseRepository.from_settings(clickhouse)),
                writer_settings,
            )
            for writer_type in WRITERS
        ]

        def stop_all() -> None:
            for writer in writers:
                writer.stop()

        signal.signal(signal.SIGTERM, lambda *_: stop_all())
        with ThreadPoolExecutor(len(writers)) as pool:
            futures = [pool.submit(writer.run) for writer in writers]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                # One writer failing stops the others, so the process exits and Compose restarts all three.
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
