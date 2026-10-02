import logging
import signal

from activityreporter.clickhouse_writer.repository import SINKS, ClickHouseRepository, KafkaRecordsRepository
from activityreporter.clickhouse_writer.service import ClickHouseWriter


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with ClickHouseRepository.from_env() as store, KafkaRecordsRepository.from_env(SINKS) as records:
        writer = ClickHouseWriter.from_env(records, store, SINKS)
        signal.signal(signal.SIGTERM, lambda *_: writer.stop())
        writer.run()


def cli() -> None:
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
