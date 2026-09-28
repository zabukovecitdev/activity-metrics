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

from activityreporter.shared.anomalies import Anomaly
from activityreporter.shared.metrics import Metric

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_CONNECTION_STRING", "localhost:9094")
KAFKA_RAW_METRICS_TOPIC = os.environ.get("KAFKA_RAW_METRICS_TOPIC", "raw_metrics")
KAFKA_ANOMALIES_TOPIC = os.environ.get("KAFKA_ANOMALIES_TOPIC", "anomalies")
KAFKA_CONSUMER_GROUP_ID = "anomaly-detector"
MAX_OUT_OF_ORDERNESS = Duration.of_seconds(5)

METRIC_FIELD_TYPES = {
    "timestamp": Types.DOUBLE(),
    "name": Types.STRING(),
    "type": Types.STRING(),
    "unit": Types.STRING(),
    "value": Types.DOUBLE(),
    "machine_id": Types.STRING(),
    "metric_id": Types.STRING(),
    "attributes": Types.MAP(Types.STRING(), Types.STRING()),
}
ANOMALY_FIELD_TYPES = {
    "metric_id": Types.STRING(),
    "machine_id": Types.STRING(),
    "metric_name": Types.STRING(),
    "timestamp": Types.DOUBLE(),
    "value": Types.DOUBLE(),
    "algorithm": Types.STRING(),
    "score": Types.DOUBLE(),
    "threshold": Types.DOUBLE(),
    "direction": Types.STRING(),
    "detected_at": Types.DOUBLE(),
    "metric_attributes": Types.MAP(Types.STRING(), Types.STRING()),
    "details": Types.MAP(Types.STRING(), Types.DOUBLE()),
    "schema_version": Types.INT(),
}


def row_type_info(record_type: type, field_types: dict):
    """Named row type in `record_type`'s field order, failing at import if the two drift apart."""
    names = [field.name for field in fields(record_type)]
    if set(names) != set(field_types):
        raise RuntimeError(
            f"Field types {sorted(field_types)} are out of sync with {record_type.__name__} {sorted(names)}"
        )
    return Types.ROW_NAMED(names, [field_types[name] for name in names])


METRIC_TYPE_INFO = row_type_info(Metric, METRIC_FIELD_TYPES)
ANOMALY_TYPE_INFO = row_type_info(Anomaly, ANOMALY_FIELD_TYPES)


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
            JsonRowDeserializationSchema.builder().type_info(METRIC_TYPE_INFO).build()
        ) \
        .build()


def kafka_anomalies_sink() -> KafkaSink:
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_ANOMALIES_TOPIC)
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(ANOMALY_TYPE_INFO).build()
            ).build()
        ).build()
