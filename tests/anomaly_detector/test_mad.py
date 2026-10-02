import pytest

from activityreporter.anomaly_detector.errors import InsufficientDataError
from activityreporter.anomaly_detector.mad import MAD


def test_mad_is_anomaly_returns_false_when_value_close_to_median():
    mad = MAD()

    values = [5, 6, 4, 8, 6, 5, 8, 5, 6]

    result = mad.is_anomaly(values)

    assert result is False


def test_mad_raises_when_too_few_values_provided():
    mad = MAD()

    values = [500]

    with pytest.raises(InsufficientDataError):
        mad.is_anomaly(values)


def test_mad_is_anomaly_returns_true_for_clear_outlier():
    mad = MAD()

    values = [1, 2, 3, 4, 5, 6, 7, 8, 30]

    result = mad.is_anomaly(values)

    assert result is True


def test_mad_is_anomaly_returns_false_for_moderate_deviation_below_threshold():
    mad = MAD()

    values = [1, 2, 3, 4, 5, 6, 7, 8, 13]

    result = mad.is_anomaly(values)

    assert result is False


def test_mad_zero_fallback_returns_false_when_last_value_equals_median():
    mad = MAD()

    values = [5, 5, 5, 5, 5]

    result = mad.is_anomaly(values)

    assert result is False


def test_mad_zero_fallback_returns_true_when_last_value_differs_from_median():
    mad = MAD()

    values = [5, 5, 5, 5, 9]

    result = mad.is_anomaly(values)

    assert result is True


def test_mad_zero_falls_back_to_scaled_mean_absolute_deviation():
    values = [5.0] * 19 + [6.0]

    score = MAD().score(values)

    assert score.median == 5.0
    assert score.scale == pytest.approx(MAD.MEAN_SIGMA * 0.05)
    assert score.score == pytest.approx(1 / (MAD.MEAN_SIGMA * 0.05))


def test_mad_score_is_zero_when_all_values_are_equal():
    score = MAD().score([5.0, 5.0, 5.0])

    assert score.score == 0.0
    assert score.scale == 0.0


def test_mad_score_is_modified_z_score_of_last_value():
    # median 5, absolute deviations median 2
    score = MAD().score([1, 3, 5, 7, 9])

    assert score.median == 5.0
    assert score.scale == pytest.approx(MAD.SIGMA * 2)
    assert score.score == pytest.approx(4 / (MAD.SIGMA * 2))


def test_min_deviation_is_the_smallest_deviation_that_reaches_the_threshold():
    values = [5.0] * 19

    assert MAD(min_deviation=2.0).score(values + [6.9]).score < MAD.THRESHOLD
    assert MAD(min_deviation=2.0).score(values + [7.0]).score == pytest.approx(MAD.THRESHOLD)


def test_min_relative_floors_the_scale_at_a_fraction_of_the_median():
    values = [1000.0, 1001.0, 999.0] * 7

    score = MAD(min_relative=0.02).score(values + [1015.0])

    assert score.spread_scale == pytest.approx(MAD.SIGMA * 1.0)
    assert score.scale == pytest.approx(0.02 * 1000.0 / MAD.THRESHOLD)
    assert score.score < MAD.THRESHOLD
    assert MAD().score(values + [1015.0]).score >= MAD.THRESHOLD


def test_floor_does_not_lower_a_wider_spread():
    values = [1, 3, 5, 7, 9]

    assert MAD(min_deviation=0.1).score(values).scale == pytest.approx(MAD.SIGMA * 2)


def test_floor_gives_equal_values_a_scale():
    score = MAD(min_deviation=1.0).score([5.0, 5.0, 5.0])

    assert score.score == 0.0
    assert score.scale == pytest.approx(1.0 / MAD.THRESHOLD)
