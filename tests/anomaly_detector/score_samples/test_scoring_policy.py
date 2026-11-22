import pytest

from activityreporter.anomaly_detector.score_samples.scoring_policy import ScoringPolicy


def build_metric(name: str) -> dict:
    return {"name": name}


@pytest.mark.parametrize("name", ["system.memory.limit", "system.uptime", "system.something.new"])
def test_only_metrics_with_detectors_are_scored(name):
    assert not ScoringPolicy.is_scored(build_metric(name))
    assert ScoringPolicy.is_scored(build_metric("system.cpu.utilization"))


def test_cpu_and_memory_are_each_scored_by_mad_and_ewma():
    for name in ("system.cpu.utilization", "system.memory.usage"):
        assert sorted(d.name for d in ScoringPolicy.detectors(name)) == ["ewma", "mad"]


def test_unscored_metric_has_no_detectors():
    assert ScoringPolicy.detectors("system.uptime") == []


def test_nab_series_are_scored_by_detectors_without_floors():
    assert ScoringPolicy.is_scored(build_metric("nab.exchange-2_cpc_results"))
    assert ScoringPolicy.detectors("nab.exchange-2_cpc_results") is ScoringPolicy.NAB_DETECTORS
