import pytest

from activityreporter.anomaly_detector.detection import MIN_VALUES_FOR_MAD, detect, is_scored
from activityreporter.anomaly_detector.mad import MAD


def build_metric(value: float, **overrides) -> dict:
    return {
        "timestamp": 1790595000.25,
        "name": "system.cpu.utilization",
        "type": "gauge",
        "unit": "%",
        "value": value,
        "machine_id": "server-42",
        "metric_id": "01a0e974-dc94-74df-ae87-e0e6064b18ef",
        "attributes": {"core": "0"},
        **overrides,
    }


BASELINE = [10.0, 11.0, 12.0, 11.0, 10.0] * 4


def test_detect_returns_anomaly_keyed_by_the_metric_sample():
    metric = build_metric(90.0)

    anomaly = detect(metric, BASELINE + [90.0], detected_at=1790595001.0)

    assert anomaly is not None
    assert (anomaly.machine_id, anomaly.metric_name, anomaly.timestamp, anomaly.metric_id) == (
        "server-42", "system.cpu.utilization", 1790595000.25, "01a0e974-dc94-74df-ae87-e0e6064b18ef",
    )
    assert anomaly.metric_attributes == {"core": "0"}
    assert anomaly.value == 90.0
    assert anomaly.algorithm == "mad"
    assert anomaly.direction == "up"
    assert anomaly.threshold == MAD.THRESHOLD
    assert anomaly.score >= MAD.THRESHOLD
    assert anomaly.details == {"median": 11.0, "scale": pytest.approx(MAD.SIGMA), "window_size": 21.0}
    assert anomaly.detected_at == 1790595001.0


def test_detect_marks_drops_as_down():
    anomaly = detect(build_metric(-50.0), BASELINE + [-50.0], detected_at=0.0)

    assert anomaly.direction == "down"


def test_detect_returns_none_for_normal_value():
    assert detect(build_metric(11.0), BASELINE + [11.0], detected_at=0.0) is None


def test_detect_returns_none_until_window_is_full():
    window = BASELINE[:MIN_VALUES_FOR_MAD - 2] + [90.0]

    assert detect(build_metric(90.0), window, detected_at=0.0) is None


@pytest.mark.parametrize("overrides", [{"type": "counter"}, {"name": "system.memory.limit"}])
def test_is_scored_skips_non_gauges_and_flags(overrides):
    assert not is_scored(build_metric(1.0, **overrides))
    assert is_scored(build_metric(1.0))
