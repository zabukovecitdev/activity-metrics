import pytest

from activityreporter.anomaly_detector.detection import DEFAULT_DETECTORS, detectors_for, evaluate, is_scored
from activityreporter.anomaly_detector.detectors import EwmaDetector, MadDetector
from activityreporter.anomaly_detector.mad import MAD

WINDOW_MS = 3_600_000


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


def by_algorithm(evaluations) -> dict:
    return {e.algorithm: e for e in evaluations}


def test_every_detector_evaluates_every_sample_once_warm():
    evaluations = evaluate(build_metric(11.0), BASELINE + [11.0], WINDOW_MS, detected_at=0.0)

    assert sorted(e.algorithm for e in evaluations) == ["ewma", "mad"]
    assert not any(e.is_anomaly for e in evaluations)


def test_evaluation_is_keyed_by_the_metric_sample():
    evaluation = by_algorithm(evaluate(build_metric(90.0), BASELINE + [90.0], WINDOW_MS, detected_at=1790595001.0))["mad"]

    assert (evaluation.machine_id, evaluation.metric_name, evaluation.timestamp, evaluation.metric_id) == (
        "server-42", "system.cpu.utilization", 1790595000.25, "01a0e974-dc94-74df-ae87-e0e6064b18ef",
    )
    assert evaluation.value == 90.0
    assert evaluation.detected_at == 1790595001.0
    assert evaluation.schema_version == 1


def test_evaluation_carries_the_detector_parameters_and_window():
    evaluation = by_algorithm(evaluate(build_metric(90.0), BASELINE + [90.0], WINDOW_MS, detected_at=0.0))["mad"]

    assert evaluation.algorithm_version == MadDetector.version
    assert evaluation.params == {**detectors_for("system.cpu.utilization")[0].params(), "window_ms": float(WINDOW_MS)}
    assert evaluation.params["min_deviation"] == 5.0


def test_mad_flags_a_spike_and_reports_its_band():
    evaluation = by_algorithm(evaluate(build_metric(90.0), BASELINE + [90.0], WINDOW_MS, detected_at=0.0))["mad"]

    assert evaluation.is_anomaly
    assert evaluation.direction == 1
    assert evaluation.threshold == MAD.THRESHOLD
    assert evaluation.baseline == 11.0
    assert evaluation.lower == pytest.approx(11.0 - MAD.THRESHOLD * MAD.SIGMA)
    assert evaluation.upper == pytest.approx(11.0 + MAD.THRESHOLD * MAD.SIGMA)
    assert evaluation.details == {
        "scale": pytest.approx(MAD.SIGMA), "spread_scale": pytest.approx(MAD.SIGMA), "window_size": 21.0,
    }


def test_mad_marks_drops_as_down():
    evaluation = by_algorithm(evaluate(build_metric(-50.0), BASELINE + [-50.0], WINDOW_MS, detected_at=0.0))["mad"]

    assert evaluation.is_anomaly
    assert evaluation.direction == -1


def test_detectors_are_skipped_until_they_have_enough_values():
    window = BASELINE[:EwmaDetector.min_points]

    evaluations = evaluate(build_metric(window[-1]), window, WINDOW_MS, detected_at=0.0)

    assert [e.algorithm for e in evaluations] == ["ewma"]
    assert evaluate(build_metric(1.0), [1.0], WINDOW_MS, detected_at=0.0) == []


@pytest.mark.parametrize(
    "overrides",
    [{"type": "counter"}, {"name": "system.memory.limit"}, {"name": "system.uptime"}],
)
def test_is_scored_skips_non_gauges_flags_and_uptime(overrides):
    assert not is_scored(build_metric(1.0, **overrides))
    assert is_scored(build_metric(1.0))


def test_unknown_metrics_get_the_default_detectors():
    assert detectors_for("system.something.new") is DEFAULT_DETECTORS


def mad_of(evaluations):
    return by_algorithm(evaluations)["mad"]


def test_cpu_moving_less_than_five_points_on_an_idle_machine_is_not_an_anomaly():
    idle = [11.0, 11.2, 10.9] * 7

    evaluation = mad_of(evaluate(build_metric(14.0), idle + [14.0], WINDOW_MS, detected_at=0.0))

    assert not evaluation.is_anomaly
    assert MadDetector().evaluate(idle + [14.0]).is_anomaly


def test_memory_moving_a_few_pages_is_not_an_anomaly():
    # What the pipeline run flagged before the floor: 32 KB on ~470 MB in use.
    memory = [492_711_936.0, 492_720_128.0, 492_703_744.0, 492_711_936.0] * 5
    metric = build_metric(492_679_168.0, name="system.memory.usage", unit="By")

    evaluation = mad_of(evaluate(metric, memory + [492_679_168.0], WINDOW_MS, detected_at=0.0))

    assert not evaluation.is_anomaly
    assert evaluation.details["scale"] > evaluation.details["spread_scale"]


def test_memory_jumping_more_than_two_percent_is_an_anomaly():
    memory = [492_711_936.0, 492_720_128.0, 492_703_744.0, 492_711_936.0] * 5
    metric = build_metric(560_000_000.0, name="system.memory.usage", unit="By")

    assert mad_of(evaluate(metric, memory + [560_000_000.0], WINDOW_MS, detected_at=0.0)).is_anomaly
