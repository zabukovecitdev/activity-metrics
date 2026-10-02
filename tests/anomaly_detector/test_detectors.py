import pytest

from activityreporter.anomaly_detector.detectors import EwmaDetector, MadDetector, sign
from activityreporter.anomaly_detector.emwa import EMWA
from activityreporter.anomaly_detector.mad import MAD

STEADY = [50.0, 52.0, 48.0, 51.0, 49.0] * 6


@pytest.mark.parametrize("x, expected", [(3.2, 1), (-0.1, -1), (0.0, 0)])
def test_sign(x, expected):
    assert sign(x) == expected


def test_mad_band_is_threshold_scales_around_the_median():
    result = MadDetector().evaluate(STEADY + [50.0])
    score = MAD().score(STEADY + [50.0])

    assert result.baseline == score.median
    assert result.lower == pytest.approx(score.median - MAD.THRESHOLD * score.scale)
    assert result.upper == pytest.approx(score.median + MAD.THRESHOLD * score.scale)
    assert not result.is_anomaly


def test_mad_band_collapses_to_the_median_when_every_value_is_equal():
    result = MadDetector().evaluate([5.0] * 25)

    assert (result.lower, result.baseline, result.upper) == (5.0, 5.0, 5.0)
    assert result.score == 0.0
    assert not result.is_anomaly


def test_ewma_has_an_upper_band_only():
    result = EwmaDetector().evaluate(STEADY + [50.0])
    score = EMWA().score(STEADY + [50.0])

    assert result.lower is None
    assert result.baseline == score.average
    assert result.upper == pytest.approx(score.average + EMWA.THRESHOLD * score.scale)
    assert result.threshold == EMWA.THRESHOLD
    assert not result.is_anomaly


def test_ewma_flags_a_spike_above_the_upper_band():
    result = EwmaDetector().evaluate(STEADY + [95.0])

    assert result.is_anomaly
    assert result.direction == 1
    assert 95.0 > result.upper


def test_ewma_does_not_flag_a_drop():
    result = EwmaDetector().evaluate(STEADY + [0.0])

    assert not result.is_anomaly
    assert result.direction == -1


@pytest.mark.parametrize("detector", [MadDetector(), EwmaDetector()], ids=["mad", "ewma"])
def test_params_are_numbers(detector):
    assert detector.params()
    assert all(isinstance(v, float) for v in detector.params().values())


def test_mad_reports_its_floors_as_params():
    params = MadDetector(min_deviation=5.0, min_relative=0.02).params()

    assert (params["min_deviation"], params["min_relative"]) == (5.0, 0.02)


def test_mad_band_widens_to_the_floor():
    result = MadDetector(min_deviation=7.0).evaluate([5.0] * 25)

    assert (result.lower, result.baseline, result.upper) == (pytest.approx(-2.0), 5.0, pytest.approx(12.0))


STEADY_MEMORY = [1_000_000_000.0, 1_000_004_096.0, 999_995_904.0] * 10


def test_ewma_default_floor_flags_a_tiny_rise_in_bytes():
    assert EMWA().score(STEADY_MEMORY + [1_000_040_960.0]).score > EMWA.THRESHOLD


def test_ewma_relative_floor_ignores_rises_below_it():
    detector = EwmaDetector(min_deviation=0.0, min_relative=0.02)

    assert not detector.evaluate(STEADY_MEMORY + [1_000_040_960.0]).is_anomaly
    assert not detector.evaluate(STEADY_MEMORY + [1_015_000_000.0]).is_anomaly
    assert detector.evaluate(STEADY_MEMORY + [1_030_000_000.0]).is_anomaly


def test_ewma_upper_band_is_at_least_the_floor_above_the_average():
    result = EwmaDetector(min_deviation=0.0, min_relative=0.02).evaluate(STEADY_MEMORY + [1_000_000_000.0])

    assert result.upper - result.baseline == pytest.approx(0.02 * result.baseline, rel=1e-3)


def test_ewma_reports_its_floors_as_params():
    params = EwmaDetector(min_deviation=5.0, min_relative=0.02).params()

    assert (params["min_deviation"], params["min_relative"]) == (5.0, 0.02)
    assert EwmaDetector().params()["min_deviation"] == EMWA.MIN_DEVIATION
