from activityreporter.anomaly_detector.algorithms.ewma import EWMA
from activityreporter.anomaly_detector.models.result import Result


class EwmaDetector:
    name = "ewma"
    min_points = EWMA.MINIMAL_POINT_COUNT

    def __init__(self, min_deviation: float = EWMA.MIN_DEVIATION, min_relative: float = 0.0):
        """See EWMA: the smallest rise, absolute or as a fraction of the average, that can be flagged."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def evaluate(self, window: list[float]) -> Result:
        score = EWMA(self.min_deviation, self.min_relative).score(window)
        return Result(
            baseline=score.average,
            # EWMA flags only rises above the average, so it has no lower band.
            lower=None,
            upper=score.average + EWMA.THRESHOLD * score.scale,
            is_anomaly=score.score > EWMA.THRESHOLD,
        )
