from dataclasses import dataclass
from typing import Protocol

from activityreporter.anomaly_detector.emwa import EMWA
from activityreporter.anomaly_detector.mad import MAD


@dataclass(frozen=True)
class Result:
    """What every algorithm reports for the latest value of a window, in the value's units:
    the line and band to draw, and whether the value is outside it."""
    baseline: float
    lower: float | None
    upper: float
    is_anomaly: bool


class Detector(Protocol):
    name: str
    min_points: int

    def evaluate(self, window: list[float]) -> Result: ...


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


class EwmaDetector:
    name = "ewma"
    min_points = EMWA.MINIMAL_POINT_COUNT

    def __init__(self, min_deviation: float = EMWA.MIN_DEVIATION, min_relative: float = 0.0):
        """See EMWA: the smallest rise, absolute or as a fraction of the average, that can be flagged."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def evaluate(self, window: list[float]) -> Result:
        score = EMWA(self.min_deviation, self.min_relative).score(window)
        return Result(
            baseline=score.average,
            # EMWA flags only rises above the average, so it has no lower band.
            lower=None,
            upper=score.average + EMWA.THRESHOLD * score.scale,
            is_anomaly=score.score > EMWA.THRESHOLD,
        )
