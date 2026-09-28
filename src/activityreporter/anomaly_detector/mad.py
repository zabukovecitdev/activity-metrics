import numpy as np

from core.errors.insufficient_data_error import InsufficientDataError


class MAD:
    SIGMA = 1.4826
    MINIMAL_POINT_COUNT = 2
    THRESHOLD = 3.5

    def is_anomaly(self, values: list[float]) -> bool:
        """
        Check whether the last value in the series deviates significantly from the
        rest, using Median Absolute Deviation (MAD).

        Requires at least 2 values.

        :param values: series to check, most recent value last
        :return: True if the last value is an anomaly
        :raises InsufficientDataError: if len(values) < 2
        """
        if len(values) < self.MINIMAL_POINT_COUNT:
            raise InsufficientDataError(values)

        last_value = values[-1]

        median = np.median(values)
        deviation = list(map(lambda x: abs(x - median), values))
        median_of_deviations =  np.median(deviation)

        if float(median_of_deviations) == 0:
            return last_value != median

        last_value_deviation = abs(last_value - median)

        scaled_mad = self.SIGMA * median_of_deviations
        modified_z_score = last_value_deviation / scaled_mad

        return modified_z_score >= self.THRESHOLD