import os
from dataclasses import fields

from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

from activityreporter.shared.metrics import ProcessedMetric

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094")
KAFKA_RAW_METRICS_TOPIC = os.environ.get("KAFKA_RAW_METRICS_TOPIC", "raw_metrics")
KAFKA_PROCESSED_METRICS_TOPIC = os.environ.get("KAFKA_PROCESSED_METRICS_TOPIC", "processed_metrics")
KAFKA_CONSUMER_GROUP_ID = "flink-metrics-aggregator"
MAX_OUT_OF_ORDERNESS = Duration.of_seconds(5)

PROCESSED_METRIC_FIELD_TYPES = {
    "timestamp": Types.DOUBLE(),
    "name": Types.STRING(),
    "type": Types.STRING(),
    "unit": Types.STRING(),
    "value": Types.DOUBLE(),
    "machine_id": Types.STRING(),
    "attributes": Types.MAP(Types.STRING(), Types.STRING()),
    "is_anomaly": Types.BOOLEAN(),
}
PROCESSED_METRIC_FIELD_NAMES = [field.name for field in fields(ProcessedMetric)]
if set(PROCESSED_METRIC_FIELD_NAMES) != set(PROCESSED_METRIC_FIELD_TYPES):
    raise RuntimeError(
        f"PROCESSED_METRIC_FIELD_TYPES {sorted(PROCESSED_METRIC_FIELD_TYPES)} is out of sync with "
        f"ProcessedMetric {sorted(PROCESSED_METRIC_FIELD_NAMES)}"
    )
PROCESSED_METRIC_TYPE_INFO = Types.ROW_NAMED(
    PROCESSED_METRIC_FIELD_NAMES,
    [PROCESSED_METRIC_FIELD_TYPES[name] for name in PROCESSED_METRIC_FIELD_NAMES],
)


class EpochSecondsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp"] * 1000)


def event_time_watermarks() -> WatermarkStrategy:
    return WatermarkStrategy.for_bounded_out_of_orderness(MAX_OUT_OF_ORDERNESS) \
        .with_timestamp_assigner(EpochSecondsTimestampAssigner())


def kafka_raw_metrics_source() -> KafkaSource:
    return KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_RAW_METRICS_TOPIC) \
        .set_group_id(KAFKA_CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(PROCESSED_METRIC_TYPE_INFO).build()
        ) \
        .build()


def kafka_processed_metrics_sink() -> KafkaSink:
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_PROCESSED_METRICS_TOPIC)
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(PROCESSED_METRIC_TYPE_INFO).build()
            ).build()
        ).build()
