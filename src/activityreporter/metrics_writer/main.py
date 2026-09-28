import logging
import signal

from connectors.timescale_connector import TimescaleConnector
from consumers.metrics_consumer import MetricsConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main() -> None:
    with TimescaleConnector.from_env() as writer:
        consumer = MetricsConsumer.from_env(writer)

        signal.signal(signal.SIGTERM, lambda *_: consumer.stop())
        consumer.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
