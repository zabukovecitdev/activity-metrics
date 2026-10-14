from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from activityreporter.anomaly_detector.flink.type_info.evaluation_type_info import EVALUATION_TYPE_INFO
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class KafkaEvaluationsSink:
    @staticmethod
    def build(kafka: KafkaSettings) -> KafkaSink:
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
