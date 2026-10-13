from __future__ import annotations

from activityreporter.clickhouse_writer.ingesters.row_values import RowValues
from activityreporter.clickhouse_writer.ingesters.topic_ingester import TopicIngester
from activityreporter.shared.models.evaluation import Evaluation
from activityreporter.shared.settings.kafka_settings import KafkaSettings


class EvaluationsIngester(TopicIngester):
    table = "evaluations"
    columns = [
        "metric_id", "machine_id", "metric_name", "timestamp", "algorithm", "baseline", "lower", "upper", "is_anomaly",
    ]
    record_type = Evaluation

    @classmethod
    def topic(cls, kafka: KafkaSettings) -> str:
        return kafka.evaluations_topic

    def to_row(self, e: Evaluation) -> list:
        return [
            RowValues.metric_uuid(e.metric_id), e.machine_id, e.metric_name, RowValues.utc(e.timestamp),
            e.algorithm, float(e.baseline), None if e.lower is None else float(e.lower), float(e.upper),
            bool(e.is_anomaly),
        ]
