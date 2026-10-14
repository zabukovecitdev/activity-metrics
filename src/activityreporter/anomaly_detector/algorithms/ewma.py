import math

from activityreporter.anomaly_detector.models.result import Result


class EWMA:
    name = "ewma"
    ALPHA = 0.1
    # Don't flag anything until the variance has had time to build up.
    WARMUP_READINGS = 10
    min_points = WARMUP_READINGS + 1
    THRESHOLD_MULTIPLIER = 3.0
    # A reading must beat both the statistical threshold and this absolute jump, by default.
    MIN_DEVIATION = 10.0
    # Anomalies still nudge the average slightly, so it can't get stuck.
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
            raise ValueError(f"EWMA needs at least {self.min_points} values, got {len(window)}")

        average = float(window[0])
        variance = 0.0
        for index, value in enumerate(window[1:], start=1):
            # Compare against the previous average: the new one would already contain the spike.
            deviation = value - average
            floor = max(self.min_deviation, self.min_relative * abs(average))
            scale = max(self.THRESHOLD_MULTIPLIER * math.sqrt(variance), floor)
            score = deviation / scale
            if index == len(window) - 1:
                return Result(
                    baseline=average,
                    lower=None,
                    upper=average + self.THRESHOLD * scale,
                    is_anomaly=score > self.THRESHOLD,
                )

            is_anomaly = index >= self.WARMUP_READINGS and score > self.THRESHOLD
            alpha = self.ALPHA * self.ANOMALY_DAMPING if is_anomaly else self.ALPHA
            average += alpha * deviation
            variance = (1 - alpha) * (variance + alpha * deviation**2)
