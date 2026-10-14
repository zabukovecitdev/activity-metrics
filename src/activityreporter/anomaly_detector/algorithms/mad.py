import numpy as np

from activityreporter.anomaly_detector.models.result import Result


class MAD:
    name = "mad"
    min_points = 20
    SIGMA = 1.4826
    # Iglewicz & Hoaglin: when over half the values equal the median, MAD is 0
    # and the mean absolute deviation, scaled to match it, stands in.
    MEAN_SIGMA = 1.253314
    THRESHOLD = 3.5

    def __init__(self, min_deviation: float = 0.0, min_relative: float = 0.0):
        """`min_deviation` (in the values' units) and `min_relative` (a fraction of the median)
        are the smallest deviations that can reach THRESHOLD.

        A series that barely moves has a tiny MAD, so any step, however small, scores far
        above the threshold. Flooring the scale at the larger of the two stops that.
        """
        self.min_deviation = min_deviation
        self.min_relative = min_relative

    def evaluate(self, window: list[float]) -> Result:
        """Modified z-score of the last (most recent) value against all of them, as the band around the median."""
        # float() throughout: numpy scalars can't be encoded by PyFlink's coders.
        median = float(np.median(window))
        deviations = np.abs(np.asarray(window, dtype=float) - median)
        spread_scale = self.SIGMA * float(np.median(deviations))
        if spread_scale == 0:
            spread_scale = self.MEAN_SIGMA * float(np.mean(deviations))
        floor = max(self.min_deviation, self.min_relative * abs(median)) / self.THRESHOLD
        scale = max(spread_scale, floor)
        margin = self.THRESHOLD * scale
        # A scale of 0 means every value is the same and there is no floor: nothing stands out.
        is_anomaly = scale > 0 and float(deviations[-1]) / scale >= self.THRESHOLD

        return Result(baseline=median, lower=median - margin, upper=median + margin, is_anomaly=is_anomaly)
