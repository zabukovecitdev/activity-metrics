import math

from activityreporter.anomaly_detector.algorithms.detector import WINDOW_POINTS
from activityreporter.anomaly_detector.models.result import Result


class EWMA:
    name = "ewma"
    ALPHA = 0.1
    min_points = WINDOW_POINTS
    THRESHOLD_MULTIPLIER = 3.0
    MIN_DEVIATION = 10.0
    ANOMALY_DAMPING = 0.1
    THRESHOLD = 1.0

    def __init__(self, min_deviation: float = MIN_DEVIATION, min_relative: float = 0.0):
        """The smallest rise that can be flagged: `min_deviation` in the values' units, or
        `min_relative` of the moving average, whichever is larger."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def evaluate(self, window: list[float]) -> Result:
        """Deviation of the last (most recent) value above the moving average, against the allowed margin.

        EWMA flags only rises above the average, so it has no lower band.
        """
        if len(window) < self.min_points:
            raise ValueError(
                f"EWMA needs at least {self.min_points} values, got {len(window)}"
            )

        average = float(window[0])
        variance = 0.0
        # The state is built from every value except the last, which is only scored against it.
        for value in window[1:-1]:
            deviation = value - average
            scale = self._scale(average, variance)
            is_anomaly = self._score(deviation, scale) > self.THRESHOLD
            alpha = self.ALPHA * self.ANOMALY_DAMPING if is_anomaly else self.ALPHA
            average += alpha * deviation
            variance = (1 - alpha) * (variance + alpha * deviation**2)

        scale = self._scale(average, variance)
        return Result(
            baseline=average,
            lower=None,
            upper=average + self.THRESHOLD * scale,
            is_anomaly=self._score(window[-1] - average, scale) > self.THRESHOLD,
        )

    def _scale(self, average: float, variance: float) -> float:
        floor = max(self.min_deviation, self.min_relative * abs(average))
        return max(self.THRESHOLD_MULTIPLIER * math.sqrt(variance), floor)

    @staticmethod
    def _score(deviation: float, scale: float) -> float:
        # A scale of 0 means a flat series and no floor: only a rise stands out, and any rise does.
        if scale > 0:
            return deviation / scale
        return math.inf if deviation > 0 else 0.0
