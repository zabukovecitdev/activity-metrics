import pytest

from activityreporter.anomaly_detector.algorithms.mad import MAD

STEADY = [50.0, 52.0, 48.0, 51.0, 49.0] * 6


def margin(values: list[float], mad: MAD | None = None) -> float:
    result = (mad or MAD()).evaluate(values)
    return result.upper - result.baseline


def test_value_close_to_the_median_is_not_an_anomaly():
    assert not MAD().evaluate([5, 6, 4, 8, 6, 5, 8, 5, 6]).is_anomaly


def test_clear_outlier_is_an_anomaly():
    assert MAD().evaluate([1, 2, 3, 4, 5, 6, 7, 8, 30]).is_anomaly


def test_moderate_deviation_below_the_threshold_is_not_an_anomaly():
    assert not MAD().evaluate([1, 2, 3, 4, 5, 6, 7, 8, 13]).is_anomaly


def test_flags_drops_too():
    assert MAD().evaluate(STEADY + [0.0]).is_anomaly


def test_band_is_symmetric_around_the_median():
    result = MAD().evaluate(STEADY + [50.0])

    assert result.baseline == 50.0
    assert result.lower is not None
    assert result.baseline - result.lower == pytest.approx(result.upper - result.baseline)
    assert not result.is_anomaly


def test_band_is_threshold_modified_z_scores_wide():
    # median 5, absolute deviations median 2
    result = MAD().evaluate([1, 3, 5, 7, 9])

    assert result.baseline == 5.0
    assert result.upper - result.baseline == pytest.approx(MAD.THRESHOLD * MAD.SIGMA * 2)
    assert not result.is_anomaly


def test_mad_of_zero_falls_back_to_scaled_mean_absolute_deviation():
    result = MAD().evaluate([5.0] * 19 + [6.0])

    assert result.baseline == 5.0
    assert result.upper - result.baseline == pytest.approx(MAD.THRESHOLD * MAD.MEAN_SIGMA * 0.05)
    assert result.is_anomaly


def test_mad_of_zero_with_last_value_on_the_median_is_not_an_anomaly():
    assert not MAD().evaluate([5, 5, 5, 5, 5]).is_anomaly


def test_mad_of_zero_with_last_value_off_the_median_is_an_anomaly():
    assert MAD().evaluate([5, 5, 5, 5, 9]).is_anomaly


def test_band_collapses_to_the_median_when_every_value_is_equal():
    result = MAD().evaluate([5.0] * 25)

    assert (result.lower, result.baseline, result.upper) == (5.0, 5.0, 5.0)
    assert not result.is_anomaly


def test_min_deviation_is_the_smallest_deviation_that_reaches_the_threshold():
    values = [5.0] * 19
    mad = MAD(min_deviation=2.0)

    assert not mad.evaluate(values + [6.9]).is_anomaly
    assert mad.evaluate(values + [7.0]).is_anomaly
    assert mad.evaluate(values + [7.0]).upper == pytest.approx(7.0)


def test_min_relative_floors_the_band_at_a_fraction_of_the_median():
    values = [1000.0, 1001.0, 999.0] * 7

    assert margin(values + [1015.0], MAD(min_relative=0.02)) == pytest.approx(0.02 * 1000.0)
    assert not MAD(min_relative=0.02).evaluate(values + [1015.0]).is_anomaly
    assert MAD().evaluate(values + [1015.0]).is_anomaly


def test_floor_does_not_narrow_a_wider_spread():
    assert margin([1, 3, 5, 7, 9], MAD(min_deviation=0.1)) == pytest.approx(MAD.THRESHOLD * MAD.SIGMA * 2)


def test_floor_gives_equal_values_a_band():
    result = MAD(min_deviation=7.0).evaluate([5.0] * 25)

    assert (result.lower, result.baseline, result.upper) == (pytest.approx(-2.0), 5.0, pytest.approx(12.0))
    assert not result.is_anomaly
