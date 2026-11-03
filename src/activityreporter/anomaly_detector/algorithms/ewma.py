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
        for index, value in enumerate(window[1:], start=1):
            # Compare against the previous average: the new one would already contain the spike.
            deviation = value - average
            floor = max(self.min_deviation, self.min_relative * abs(average))
            scale = max(self.THRESHOLD_MULTIPLIER * math.sqrt(variance), floor)
            # A scale of 0 means a flat series and no floor: only a rise stands out, and any rise does.
            score = (
                deviation / scale if scale > 0 else math.inf if deviation > 0 else 0.0
            )
            if index == len(window) - 1:
                return Result(
                    baseline=average,
                    lower=None,
                    upper=average + self.THRESHOLD * scale,
                    is_anomaly=score > self.THRESHOLD,
                )

            is_anomaly = score > self.THRESHOLD
            alpha = self.ALPHA * self.ANOMALY_DAMPING if is_anomaly else self.ALPHA
            average += alpha * deviation
            variance = (1 - alpha) * (variance + alpha * deviation**2)

        raise AssertionError("unreachable: the loop returns on the last value")
