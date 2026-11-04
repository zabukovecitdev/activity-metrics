from typing import Protocol

from activityreporter.anomaly_detector.models.result import Result


# A count, not a time span, so every detector sees the same number of values at any sample rate.
# A detector scores only on a full window.
WINDOW_POINTS = 20


class Detector(Protocol):
    name: str
    min_points: int

    def evaluate(self, window: list[float]) -> Result: ...
