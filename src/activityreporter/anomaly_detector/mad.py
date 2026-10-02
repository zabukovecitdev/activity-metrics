from dataclasses import dataclass

import numpy as np

from activityreporter.anomaly_detector.errors import InsufficientDataError


@dataclass(frozen=True)
class Score:
    score: float
    median: float
    # The scale the score is divided by, after the floor.
    scale: float
    # The scale from the values alone, before the floor.
    spread_scale: float = 0.0


class MAD:
    SIGMA = 1.4826
    # Iglewicz & Hoaglin: when over half the values equal the median, MAD is 0
    # and the mean absolute deviation, scaled to match it, stands in.
    MEAN_SIGMA = 1.253314
    MINIMAL_POINT_COUNT = 2
    THRESHOLD = 3.5

    def __init__(self, min_deviation: float = 0.0, min_relative: float = 0.0):
        """`min_deviation` (in the values' units) and `min_relative` (a fraction of the median)
        are the smallest deviations that can reach THRESHOLD.

        A series that barely moves has a tiny MAD, so any step, however small, scores far
        above the threshold. Flooring the scale at the larger of the two stops that.
        """
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def score(self, values: list[float]) -> Score:
        """Modified z-score of the last (most recent) value against all of them."""
        if len(values) < self.MINIMAL_POINT_COUNT:
            raise InsufficientDataError(values)

        # float() throughout: numpy scalars can't be encoded by PyFlink's coders.
        median = float(np.median(values))
        deviations = np.abs(np.asarray(values, dtype=float) - median)
        spread_scale = self.SIGMA * float(np.median(deviations))
        if spread_scale == 0:
            spread_scale = self.MEAN_SIGMA * float(np.mean(deviations))
        floor = max(self.min_deviation, self.min_relative * abs(median)) / self.THRESHOLD
        scale = max(spread_scale, floor)
        if scale == 0:
            # Every value is the same and there is no floor.
            return Score(score=0.0, median=median, scale=0.0)

        return Score(score=float(deviations[-1]) / scale, median=median, scale=scale, spread_scale=spread_scale)

    def is_anomaly(self, values: list[float]) -> bool:
        return self.score(values).score >= self.THRESHOLD
