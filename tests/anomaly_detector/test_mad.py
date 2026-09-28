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