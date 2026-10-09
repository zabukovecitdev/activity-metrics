from dataclasses import fields

from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

from activityreporter.shared.models import Evaluation, Metric
from activityreporter.shared.settings import KafkaSettings

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
EVALUATION_FIELD_TYPES = {
    "metric_id": Types.STRING(),
    "machine_id": Types.STRING(),
    "metric_name": Types.STRING(),
    "timestamp": Types.DOUBLE(),
    "algorithm": Types.STRING(),
    "baseline": Types.DOUBLE(),
    "lower": Types.DOUBLE(),
    "upper": Types.DOUBLE(),
    "is_anomaly": Types.BOOLEAN(),
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
EVALUATION_TYPE_INFO = row_type_info(Evaluation, EVALUATION_FIELD_TYPES)


class EpochSecondsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp"] * 1000)


def event_time_watermarks() -> WatermarkStrategy:
    return WatermarkStrategy.for_bounded_out_of_orderness(MAX_OUT_OF_ORDERNESS) \
        .with_timestamp_assigner(EpochSecondsTimestampAssigner())


def kafka_raw_metrics_source(kafka: KafkaSettings) -> KafkaSource:
    return KafkaSource.builder() \
        .set_bootstrap_servers(kafka.connection_string) \
        .set_topics(kafka.raw_metrics_topic) \
        .set_group_id(KAFKA_CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(METRIC_TYPE_INFO).build()
        ) \
        .build()


def kafka_evaluations_sink(kafka: KafkaSettings) -> KafkaSink:
    # Checkpoints store the source offset. DeliveryGuarantee.NONE, the default,
    # completes a checkpoint without waiting for the producer, so a restore
    # never recomputes evaluations Kafka did not ack. AT_LEAST_ONCE flushes on
    # the checkpoint. A duplicate from a replay collapses in ReplacingMergeTree.
    return KafkaSink.builder() \
        .set_bootstrap_servers(kafka.connection_string) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(kafka.evaluations_topic)
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(EVALUATION_TYPE_INFO).build()
            ).build()
        ).build()
