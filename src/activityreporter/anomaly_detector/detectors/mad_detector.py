from activityreporter.anomaly_detector.algorithms.mad import MAD
from activityreporter.anomaly_detector.models.result import Result


class MadDetector:
    name = "mad"
    min_points = 20

    def __init__(self, min_deviation: float = 0.0, min_relative: float = 0.0):
        """See MAD: the smallest deviation, absolute or as a fraction of the median, that can be flagged."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def evaluate(self, window: list[float]) -> Result:
        score = MAD(self.min_deviation, self.min_relative).score(window)
        margin = MAD.THRESHOLD * score.scale
        return Result(
            baseline=score.median,
            lower=score.median - margin,
            upper=score.median + margin,
            is_anomaly=score.score >= MAD.THRESHOLD,
        )
