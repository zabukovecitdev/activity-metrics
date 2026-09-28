import logging
import signal

from activityreporter.metrics_writer.repository import KafkaProcessedMetricsRepository, TimescaleMetricsRepository
from activityreporter.metrics_writer.service import MetricsWriter


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with TimescaleMetricsRepository.from_env() as timescale_metrics, \
            KafkaProcessedMetricsRepository.from_env() as processed_metrics:
        writer = MetricsWriter.from_env(processed_metrics, timescale_metrics)
        signal.signal(signal.SIGTERM, lambda *_: writer.stop())
        writer.run()


def cli() -> None:
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
