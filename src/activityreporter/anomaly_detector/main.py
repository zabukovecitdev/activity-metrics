import os
import sys
from dataclasses import fields
from pathlib import Path

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.mad import MAD
from activityreporter.shared.metrics import ProcessedMetric

ONE_HOUR_MS = 60 * 60 * 1000
MIN_VALUES_FOR_MAD = 20
# MAD only makes sense on gauges: a counter only ever grows, so its raw value
# drifts away from the window median and would need a rate first. Among
# gauges, MAD is meaningless for a 0/1 flag or a value that barely changes:
# its median absolute deviation is 0, so every step (plugging in the charger,
# battery dropping 1%) would be flagged. These pass through with
# is_anomaly=False and keep no state.
NOT_ANOMALY_SCORED = {"system.battery.charging", "system.battery.utilization", "system.memory.limit"}

# Same jar version baked into the jobmanager/taskmanager image by
# docker/flink/Dockerfile — local runs need it on the classpath too, since
# apache-flink's bundled jars don't include the Kafka connector. The docker
# submitter sets this to "" so the image's copy isn't loaded a second time.
KAFKA_CONNECTOR_JAR = os.environ.get(
    "KAFKA_CONNECTOR_JAR",
    str(Path(__file__).parent / "lib" / "flink-sql-connector-kafka-3.2.0-1.19.jar"),
)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094")

# Metric.timestamp is `time.time()` — epoch seconds, not an ISO-8601
# instant — so the row type must be DOUBLE, and event time is derived from it
# explicitly instead of relying on Kafka record timestamps.
METRIC_FIELD_TYPES = {
    "timestamp": Types.DOUBLE(),
    "name": Types.STRING(),
    "type": Types.STRING(),
    "unit": Types.STRING(),
    "value": Types.DOUBLE(),
    "machine_id": Types.STRING(),
    "attributes": Types.MAP(Types.STRING(), Types.STRING()),
    "is_anomaly": Types.BOOLEAN(),
}
# The row schema follows ProcessedMetric, which the consumer builds from this
# job's output. `is_anomaly` isn't produced by the collector — pyflink's Row
# has a fixed schema, so the field has to be declared upfront (JSON
# deserialization leaves it at its default) for AnomalyDetector to set it.
METRIC_FIELD_NAMES = [field.name for field in fields(ProcessedMetric)]
if set(METRIC_FIELD_NAMES) != set(METRIC_FIELD_TYPES):
    raise RuntimeError(
        f"METRIC_FIELD_TYPES {sorted(METRIC_FIELD_TYPES)} is out of sync with "
        f"ProcessedMetric {sorted(METRIC_FIELD_NAMES)}"
    )
METRIC_TYPE_INFO = Types.ROW_NAMED(METRIC_FIELD_NAMES, [METRIC_FIELD_TYPES[name] for name in METRIC_FIELD_NAMES])


class MetricTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp"] * 1000)


class AnomalyDetector(KeyedProcessFunction):
    """Evaluates every incoming metric against the last hour of values of
    the same metric on the same machine (the stream is keyed by
    (machine_id, name)), instead of batching a verdict once per sliding-window
    firing (which only scores whichever value happens to be last when the
    window closes, and can re-emit the same value across several firings)."""

    def open(self, runtime_context: RuntimeContext):
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.TUPLE([Types.LONG(), Types.DOUBLE()]))
        )
        self.mad_detector = MAD()

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if value["type"] != "gauge" or value["name"] in NOT_ANOMALY_SCORED:
            value["is_anomaly"] = False
            yield value
            return

        event_time = ctx.timestamp()
        cutoff = event_time - ONE_HOUR_MS

        window = [(t, v) for t, v in self.recent_values.get() if t >= cutoff]
        window.append((event_time, value["value"]))
        self.recent_values.update(window)

        values = [v for _, v in window]
        if len(values) < MIN_VALUES_FOR_MAD:
            value["is_anomaly"] = False
        else:
            value["is_anomaly"] = self.mad_detector.is_anomaly(values)
        yield value


def detect_anomalies():
    # Passing jars via Configuration at env creation (rather than the
    # add_jars() escape hatch) avoids a context-classloader race that can
    # leave the Kafka connector classes unresolved when combined with
    # set_python_executable below.
    config = Configuration()
    if KAFKA_CONNECTOR_JAR:
        # as_uri() keeps a relative path from becoming file://relative, which
        # the JVM treats as a host name and fails to load the connector.
        config.set_string("pipeline.jars", Path(KAFKA_CONNECTOR_JAR).resolve().as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # Local Python UDF workers (key_by/process) are spawned as a subprocess
    # via plain `python` on PATH, which may not be this venv — pin it
    # explicitly or the job dies immediately with ModuleNotFoundError: pyflink.
    env.set_python_executable(sys.executable)

    # The raw metrics topic has a single partition (docker-compose default),
    # so one of the two Kafka source subtasks sits idle; the parallelism pays
    # off after key_by, where the (machine_id, name) keys and their MAD state
    # are split across two AnomalyDetector subtasks.
    env.set_parallelism(2)

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(METRIC_TYPE_INFO) \
        .build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(os.environ.get("KAFKA_RAW_METRICS_TOPIC", "raw_metrics")) \
        .set_group_id("flink-metrics-aggregator") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(deserialization_schema) \
        .build()

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(MetricTimestampAssigner())

    metrics = env.from_source(source, watermark_strategy, "Kafka Source")

    # Kafka partitions by machine_id (per-machine ordering); state here is
    # finer-grained so each metric gets its own MAD history.
    anomaly_processed_metrics = (metrics.key_by(lambda t: (t["machine_id"], t["name"]),
                                                key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
                                 .process(AnomalyDetector(), output_type=METRIC_TYPE_INFO))

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(os.environ.get("KAFKA_PROCESSED_METRICS_TOPIC", "processed_metrics"))
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(METRIC_TYPE_INFO).build()
            ).build()
        ).build()

    anomaly_processed_metrics.print()
    anomaly_processed_metrics.sink_to(sink)

    env.execute("Anomaly Detection")


if __name__ == "__main__":
    detect_anomalies()
