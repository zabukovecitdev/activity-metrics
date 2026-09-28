import pytest

from core.errors.insufficient_data_error import InsufficientDataError
from core.mad import MAD


def test_mad_is_anomaly_returns_false_when_value_close_to_median():
    mad = MAD()

    values = [5, 6, 4, 8, 6, 5, 8, 5, 6]

    result = mad.is_anomaly(values)

    assert bool(result) is False


def test_mad_raises_when_too_few_values_provided():
    mad = MAD()

    values = [500]

    with pytest.raises(InsufficientDataError):
        mad.is_anomaly(values)


def test_mad_is_anomaly_returns_true_for_clear_outlier():
    mad = MAD()

    values = [1, 2, 3, 4, 5, 6, 7, 8, 30]

    result = mad.is_anomaly(values)

    assert bool(result) is True


def test_mad_is_anomaly_returns_false_for_moderate_deviation_below_threshold():
    mad = MAD()

    values = [1, 2, 3, 4, 5, 6, 7, 8, 13]

    result = mad.is_anomaly(values)

    assert bool(result) is False


def test_mad_zero_fallback_returns_false_when_last_value_equals_median():
    mad = MAD()

    values = [5, 5, 5, 5, 5]

    result = mad.is_anomaly(values)

    assert bool(result) is False


def test_mad_zero_fallback_returns_true_when_last_value_differs_from_median():
    mad = MAD()

    values = [5, 5, 5, 5, 9]

    result = mad.is_anomaly(values)

    assert bool(result) is True