import sys
from pathlib import Path

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment

from activityreporter.anomaly_detector.flink.processors.anomaly_detection_function import (
    AnomalyDetectionFunction,
)
from activityreporter.anomaly_detector.flink.sinks.kafka_evaluations_sink import (
    KafkaEvaluationsSink,
)
from activityreporter.anomaly_detector.flink.sources.kafka_metrics_source import (
    KafkaMetricsSource,
)
from activityreporter.anomaly_detector.flink.type_info.evaluation_type_info import (
    EVALUATION_TYPE_INFO,
)
from activityreporter.shared.settings.kafka_settings import KafkaSettings

PARALLELISM = 2


def main() -> None:
    # Jars via Configuration, not add_jars(): add_jars() races with set_python_executable on the classloader.
    """Builds the Flink job that scores metrics and writes the evaluations to Kafka.

    Enables checkpointing, then starts the job. Blocks until the job stops.
    """
    kafka = KafkaSettings()
    config = Configuration()
    if kafka.connector_jar:
        # as_uri() keeps a relative path from becoming file://relative, which
        # the JVM treats as a host name and fails to load the connector.
        config.set_string("pipeline.jars", Path(kafka.connector_jar).resolve().as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)
    # Python UDF workers otherwise start with whatever `python` is on PATH, which may lack pyflink.
    env.set_python_executable(sys.executable)
    env.set_parallelism(PARALLELISM)

    # Without state.checkpoints.dir Flink keeps checkpoints on the JobManager heap; see docker-compose.yml.
    # 10 s matches the scrape interval: a failure replays about one record per key, and replay is idempotent.
    env.enable_checkpointing(10_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    # The default of 0 restarts the job on one slow or failed checkpoint.
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(3)

    # No watermarks: the process function scores in arrival order and uses no timers or windows.
    raw_metrics = env.from_source(
        KafkaMetricsSource.build(kafka),
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )
    evaluations = raw_metrics.key_by(
        lambda m: (m["machine_id"], m["name"]),
        key_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
    ).process(AnomalyDetectionFunction(), output_type=EVALUATION_TYPE_INFO)

    evaluations.sink_to(KafkaEvaluationsSink.build(kafka))

    env.execute("Anomaly Detection")


def cli() -> None:
    """Runs `main` and ends without an error on Ctrl+C."""
    try:
        main()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
