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


def test_relative_floor_scores_a_series_that_starts_at_zero():
    # No absolute floor: while the average is still 0 the margin is 0, which used to divide by zero.
    ewma = EWMA(min_deviation=0.0, min_relative=0.02)

    flat = ewma.evaluate([0.0] * 11)
    assert not flat.is_anomaly
    assert (flat.baseline, flat.upper) == (0.0, 0.0)

    jump = ewma.evaluate([0.0] * 10 + [500_000_000.0])
    assert jump.is_anomaly
    assert (jump.baseline, jump.upper) == (0.0, 0.0)


def test_upper_band_is_at_least_the_floor_above_the_average():
    result = EWMA(min_deviation=0.0, min_relative=0.02).evaluate(STEADY_MEMORY + [1_000_000_000.0])

    assert result.upper - result.baseline == pytest.approx(0.02 * result.baseline, rel=1e-3)
