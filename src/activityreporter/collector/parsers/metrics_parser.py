import uuid

from activityreporter.shared.models.metric import Metric
from activityreporter.shared.models.metrics_response import MetricsResponse


class MetricsParser:
    @staticmethod
    def parse(body: str) -> list[Metric]:
        """One Metric per observation in an agent's /v1/metrics body. Raises ValidationError if it isn't a MetricsResponse."""
        response = MetricsResponse.model_validate_json(body)
        timestamp = response.timestamp.timestamp()
        return [
            Metric(
                **m.model_dump(),
                timestamp=timestamp,
                machine_id=response.machine_id,
                metric_id=str(uuid.uuid7()),
            )
            for m in response.metrics
        ]
