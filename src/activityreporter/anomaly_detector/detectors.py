from dataclasses import dataclass
from typing import Protocol

from activityreporter.anomaly_detector.emwa import EMWA
from activityreporter.anomaly_detector.mad import MAD


def sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


@dataclass(frozen=True)
class Result:
    """What every algorithm reports for the latest value of a window, in the value's units."""
    baseline: float | None
    lower: float | None
    upper: float | None
    score: float
    threshold: float
    is_anomaly: bool
    direction: int
    details: dict[str, float]


class Detector(Protocol):
    name: str
    # Bump when a code change alters results, so old and new evaluations can be told apart.
    version: int
    min_points: int

    def params(self) -> dict[str, float]: ...

    def evaluate(self, window: list[float]) -> Result: ...


class MadDetector:
    name = "mad"
    version = 1
    min_points = 20

    def __init__(self, min_deviation: float = 0.0, min_relative: float = 0.0):
        """See MAD: the smallest deviation, absolute or as a fraction of the median, that can be flagged."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def params(self) -> dict[str, float]:
        return {
            "threshold": MAD.THRESHOLD,
            "sigma": MAD.SIGMA,
            "min_points": float(self.min_points),
            "min_deviation": self.min_deviation,
            "min_relative": self.min_relative,
        }

    def evaluate(self, window: list[float]) -> Result:
        score = MAD(self.min_deviation, self.min_relative).score(window)
        margin = MAD.THRESHOLD * score.scale
        return Result(
            baseline=score.median,
            lower=score.median - margin,
            upper=score.median + margin,
            score=score.score,
            threshold=MAD.THRESHOLD,
            is_anomaly=score.score >= MAD.THRESHOLD,
            direction=sign(window[-1] - score.median),
            details={"scale": score.scale, "spread_scale": score.spread_scale, "window_size": float(len(window))},
        )


class EwmaDetector:
    name = "ewma"
    version = 1
    min_points = EMWA.MINIMAL_POINT_COUNT

    def __init__(self, min_deviation: float = EMWA.MIN_DEVIATION, min_relative: float = 0.0):
        """See EMWA: the smallest rise, absolute or as a fraction of the average, that can be flagged."""
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def params(self) -> dict[str, float]:
        return {
            "alpha": EMWA.ALPHA,
            "warmup_readings": float(EMWA.WARMUP_READINGS),
            "threshold_multiplier": EMWA.THRESHOLD_MULTIPLIER,
            "min_deviation": self.min_deviation,
            "min_relative": self.min_relative,
            "anomaly_damping": EMWA.ANOMALY_DAMPING,
            "threshold": EMWA.THRESHOLD,
        }

    def evaluate(self, window: list[float]) -> Result:
        score = EMWA(self.min_deviation, self.min_relative).score(window)
        return Result(
            baseline=score.average,
            # EMWA flags only rises above the average, so it has no lower band.
            lower=None,
            upper=score.average + EMWA.THRESHOLD * score.scale,
            score=score.score,
            threshold=EMWA.THRESHOLD,
            is_anomaly=score.score > EMWA.THRESHOLD,
            direction=sign(score.score),
            details={"scale": score.scale, "window_size": float(len(window))},
        )
