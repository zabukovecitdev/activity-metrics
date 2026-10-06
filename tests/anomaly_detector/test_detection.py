import pytest

from activityreporter.anomaly_detector.detection import DEFAULT_DETECTORS, detectors_for, evaluate, is_scored
from activityreporter.anomaly_detector.detectors import MadDetector
from activityreporter.shared.models import Evaluation


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
IDLE = [11.0, 11.2, 10.9] * 7
MEMORY = [492_711_936.0, 492_720_128.0, 492_703_744.0, 492_711_936.0] * 5


def by_algorithm(metric: dict, window: list[float]) -> dict[str, Evaluation]:
    return {e.algorithm: e for e in evaluate(metric, window + [metric["value"]])}


def test_every_detector_evaluates_every_sample_once_warm():
    evaluations = evaluate(build_metric(11.0), BASELINE + [11.0])

    assert sorted(e.algorithm for e in evaluations) == ["ewma", "mad"]
    assert not any(e.is_anomaly for e in evaluations)


def test_evaluation_is_keyed_by_the_metric_sample():
    evaluation = by_algorithm(build_metric(90.0), BASELINE)["mad"]

    assert (evaluation.metric_id, evaluation.machine_id, evaluation.metric_name, evaluation.timestamp) == (
        "01a0e974-dc94-74df-ae87-e0e6064b18ef", "server-42", "system.cpu.utilization", 1790595000.25,
    )


def test_mad_flags_a_spike_and_reports_its_band():
    evaluation = by_algorithm(build_metric(90.0), BASELINE)["mad"]

    # The CPU floor (10 points) is wider than the spread, so the band is the median +- 10.
    assert evaluation == Evaluation(
        metric_id="01a0e974-dc94-74df-ae87-e0e6064b18ef",
        machine_id="server-42",
        metric_name="system.cpu.utilization",
        timestamp=1790595000.25,
        algorithm="mad",
        baseline=11.0,
        lower=pytest.approx(1.0),
        upper=pytest.approx(21.0),
        is_anomaly=True,
    )


def test_mad_flags_drops_too():
    assert by_algorithm(build_metric(-50.0), BASELINE)["mad"].is_anomaly


def test_ewma_has_no_lower_band():
    evaluation = by_algorithm(build_metric(11.0), BASELINE)["ewma"]

    assert evaluation.lower is None
    assert evaluation.upper > evaluation.baseline


def test_detectors_are_skipped_until_they_have_enough_values():
    window = BASELINE[:11]

    assert [e.algorithm for e in evaluate(build_metric(window[-1]), window)] == ["ewma"]
    assert evaluate(build_metric(1.0), [1.0]) == []


def test_unknown_metrics_get_the_default_detectors():
    assert detectors_for("system.something.new") is DEFAULT_DETECTORS


def test_cpu_moving_less_than_ten_points_on_an_idle_machine_is_not_an_anomaly():
    assert not by_algorithm(build_metric(19.0), IDLE)["mad"].is_anomaly
    assert not by_algorithm(build_metric(19.0), IDLE)["ewma"].is_anomaly
    # Without the floor MAD would flag it.
    assert MadDetector().evaluate(IDLE + [19.0]).is_anomaly


def test_cpu_rise_of_more_than_ten_points_on_an_idle_machine_is_an_anomaly():
    evaluations = by_algorithm(build_metric(22.0), IDLE)

    assert evaluations["mad"].is_anomaly
    assert evaluations["ewma"].is_anomaly


def test_memory_moving_a_few_pages_is_not_an_anomaly():
    # What the pipeline run flagged before the floor: 32 KB on ~470 MB in use, down and up.
    for value in (492_679_168.0, 492_744_704.0):
        evaluations = by_algorithm(build_metric(value, name="system.memory.usage", unit="By"), MEMORY)
        assert not evaluations["mad"].is_anomaly
        assert not evaluations["ewma"].is_anomaly


def test_memory_jumping_more_than_two_percent_is_an_anomaly():
    evaluations = by_algorithm(build_metric(560_000_000.0, name="system.memory.usage", unit="By"), MEMORY)

    assert evaluations["mad"].is_anomaly
    assert evaluations["ewma"].is_anomaly


@pytest.mark.parametrize(
    "overrides",
    [
        {"type": "counter"},
        {"name": "system.memory.limit"},
        {"name": "system.uptime"},
    ],
)
def test_is_scored_skips_non_gauges_flags_and_uptime(overrides):
    assert not is_scored(build_metric(1.0, **overrides))
    assert is_scored(build_metric(1.0))
