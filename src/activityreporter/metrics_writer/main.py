import logging
import signal

from activityreporter.metrics_writer.repository import KafkaProcessedMetricsRepository, ClickHouseMetricsRepository
from activityreporter.metrics_writer.service import MetricsWriter


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with ClickHouseMetricsRepository.from_env() as metrics_store, \
            KafkaProcessedMetricsRepository.from_env() as processed_metrics:
        writer = MetricsWriter.from_env(processed_metrics, metrics_store)
        signal.signal(signal.SIGTERM, lambda *_: writer.stop())
        writer.run()


def cli() -> None:
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
