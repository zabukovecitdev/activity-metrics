from typing import Protocol

from activity_metrics.anomaly_detector.models.result import Result


class Detector(Protocol):
    name: str
    min_points: int

    def evaluate(self, window: list[float]) -> Result: ...
