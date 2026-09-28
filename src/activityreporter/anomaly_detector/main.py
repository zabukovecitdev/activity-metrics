import os
import sys
from pathlib import Path

from pyflink.common import Configuration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from activityreporter.anomaly_detector.repository import (
    ANOMALY_TYPE_INFO,
    event_time_watermarks,
    kafka_anomalies_sink,
    kafka_raw_metrics_source,
)
from activityreporter.anomaly_detector.service import AnomalyDetector

KAFKA_CONNECTOR_JAR = os.environ.get("KAFKA_CONNECTOR_JAR", "")
PARALLELISM = 2


def main() -> None:
    # Jars via Configuration, not add_jars(): add_jars() races with set_python_executable on the classloader.
    config = Configuration()
    if KAFKA_CONNECTOR_JAR:
        # as_uri() keeps a relative path from becoming file://relative, which
        # the JVM treats as a host name and fails to load the connector.
        config.set_string("pipeline.jars", Path(KAFKA_CONNECTOR_JAR).resolve().as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)
    # Python UDF workers otherwise start with whatever `python` is on PATH, which may lack pyflink.
    env.set_python_executable(sys.executable)
    env.set_parallelism(PARALLELISM)

    raw_metrics = env.from_source(kafka_raw_metrics_source(), event_time_watermarks(), "Kafka Source")
    anomalies = raw_metrics \
        .key_by(lambda m: (m["machine_id"], m["name"]), key_type=Types.TUPLE([Types.STRING(), Types.STRING()])) \
        .process(AnomalyDetector(), output_type=ANOMALY_TYPE_INFO)

    anomalies.print()
    anomalies.sink_to(kafka_anomalies_sink())

    env.execute("Anomaly Detection")


def cli() -> None:
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
