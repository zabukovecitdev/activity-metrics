import pytest

from activityreporter.anomaly_detector.algorithms.ewma import EWMA

STEADY = [50.0, 52.0, 48.0, 51.0, 49.0] * 6
STEADY_MEMORY = [1_000_000_000.0, 1_000_004_096.0, 999_995_904.0] * 10


def test_has_an_upper_band_only():
    result = EWMA().evaluate(STEADY + [50.0])

    assert result.lower is None
    assert result.upper > result.baseline
    assert not result.is_anomaly


def test_flags_a_spike_above_the_upper_band():
    result = EWMA().evaluate(STEADY + [95.0])

    assert result.is_anomaly
    assert 95.0 > result.upper


def test_does_not_flag_a_drop():
    assert not EWMA().evaluate(STEADY + [0.0]).is_anomaly


def test_raises_until_warmed_up():
    with pytest.raises(ValueError):
        EWMA().evaluate(STEADY[:EWMA.min_points - 1])


def test_default_floor_flags_a_tiny_rise_in_bytes():
    assert EWMA().evaluate(STEADY_MEMORY + [1_000_040_960.0]).is_anomaly


def test_relative_floor_ignores_rises_below_it():
    ewma = EWMA(min_deviation=0.0, min_relative=0.02)

    assert not ewma.evaluate(STEADY_MEMORY + [1_000_040_960.0]).is_anomaly
    assert not ewma.evaluate(STEADY_MEMORY + [1_015_000_000.0]).is_anomaly
    assert ewma.evaluate(STEADY_MEMORY + [1_030_000_000.0]).is_anomaly


def test_flat_series_without_a_floor_flags_only_a_rise():
    ewma = EWMA(min_deviation=0.0)

    assert not ewma.evaluate([5.0] * 30).is_anomaly
    assert ewma.evaluate([5.0] * 30 + [5.1]).is_anomaly
    assert not ewma.evaluate([5.0] * 30 + [4.9]).is_anomaly


def test_upper_band_is_at_least_the_floor_above_the_average():
    result = EWMA(min_deviation=0.0, min_relative=0.02).evaluate(STEADY_MEMORY + [1_000_000_000.0])

    assert result.upper - result.baseline == pytest.approx(0.02 * result.baseline, rel=1e-3)


def test_average_is_seeded_with_the_first_value():
    result = EWMA().evaluate([100.0] + [50.0] * 30)

    assert result.baseline == pytest.approx(50.0 + 50.0 * 0.9**29)


def test_flat_series_with_a_small_floor_ignores_a_rise_below_it():
    ewma = EWMA(min_deviation=0.5)

    assert not ewma.evaluate([5.0] * 30 + [5.4]).is_anomaly
    assert ewma.evaluate([5.0] * 30 + [5.6]).is_anomaly


def test_reading_exactly_at_the_threshold_is_not_an_anomaly():
    assert not EWMA(min_deviation=0.5).evaluate([5.0] * 30 + [5.5]).is_anomaly


def test_an_anomaly_after_warmup_only_nudges_the_average():
    # The spike at the first scored index (10) is damped: 5.0 + 0.1 * 0.1 * 1.0, then four updates decay it.
    result = EWMA(min_deviation=0.5).evaluate([5.0] * 10 + [6.0] + [5.0] * 5)

    assert result.baseline == pytest.approx(5.0 + 0.01 * 0.9**4)


def test_a_reading_exactly_at_the_threshold_is_not_damped():
    result = EWMA(min_deviation=0.5).evaluate([5.0] * 20 + [5.5] + [5.0] * 4)

    assert result.baseline == pytest.approx(5.0 + 0.05 * 0.9**3)
