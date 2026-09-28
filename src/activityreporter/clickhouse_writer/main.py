import logging
import signal

from activityreporter.clickhouse_writer.repository import (
    ANOMALIES,
    METRICS,
    ClickHouseRepository,
    KafkaRecordsRepository,
    Sink,
)
from activityreporter.clickhouse_writer.service import ClickHouseWriter


def main(sink: Sink) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with ClickHouseRepository.from_env(sink) as store, KafkaRecordsRepository.from_env(sink) as records:
        writer = ClickHouseWriter.from_env(records, store)
        signal.signal(signal.SIGTERM, lambda *_: writer.stop())
        writer.run()


def run(sink: Sink) -> None:
    try:
        main(sink)
    except KeyboardInterrupt:
        pass


def metrics_cli() -> None:
    run(METRICS)


def anomalies_cli() -> None:
    run(ANOMALIES)


if __name__ == "__main__":
    metrics_cli()
