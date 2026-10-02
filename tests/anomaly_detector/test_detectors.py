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
