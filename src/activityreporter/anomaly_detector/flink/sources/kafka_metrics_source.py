from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from activityreporter.anomaly_detector.flink.type_info.metric_type_info import METRIC_TYPE_INFO
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class KafkaMetricsSource:
    """raw_metrics source. One consumer group, starting at the earliest offset so a new job reads history."""

    GROUP_ID = "anomaly-detector"

    @staticmethod
    def build(kafka: KafkaSettings) -> KafkaSource:
        return KafkaSource.builder() \
            .set_bootstrap_servers(kafka.connection_string) \
            .set_topics(kafka.raw_metrics_topic) \
            .set_group_id(KafkaMetricsSource.GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(
                JsonRowDeserializationSchema.builder().type_info(METRIC_TYPE_INFO).build()
            ) \
            .build()
