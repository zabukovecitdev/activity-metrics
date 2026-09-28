from dataclasses import dataclass

import numpy as np

from activityreporter.anomaly_detector.errors import InsufficientDataError


@dataclass(frozen=True)
class Score:
    score: float
    median: float
    scale: float


class MAD:
    SIGMA = 1.4826
    # Iglewicz & Hoaglin: when over half the values equal the median, MAD is 0
    # and the mean absolute deviation, scaled to match it, stands in.
    MEAN_SIGMA = 1.253314
    MINIMAL_POINT_COUNT = 2
    THRESHOLD = 3.5

    def score(self, values: list[float]) -> Score:
        """Modified z-score of the last (most recent) value against all of them."""
        if len(values) < self.MINIMAL_POINT_COUNT:
            raise InsufficientDataError(values)

        # float() throughout: numpy scalars can't be encoded by PyFlink's coders.
        median = float(np.median(values))
        deviations = np.abs(np.asarray(values, dtype=float) - median)
        scale = self.SIGMA * float(np.median(deviations))
        if scale == 0:
            scale = self.MEAN_SIGMA * float(np.mean(deviations))
        if scale == 0:
            # Every value is the same.
            return Score(score=0.0, median=median, scale=0.0)

        return Score(score=float(deviations[-1]) / scale, median=median, scale=scale)

    def is_anomaly(self, values: list[float]) -> bool:
        return self.score(values).score >= self.THRESHOLD
