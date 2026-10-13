import json

import pytest

from activityreporter.collector.parsers.metrics_parser import MetricsParser


METRICS_PAYLOAD = {
    "machine_id": "m1",
    "timestamp": "2026-10-01T06:30:00+00:00",
    "metrics": [{"name": "system.cpu.utilization", "type": "gauge", "unit": "%", "value": "15.5"}],
}


def test_metrics_are_parsed_from_the_agent_response():
    [metric] = MetricsParser.parse(json.dumps(METRICS_PAYLOAD))
    assert (metric.machine_id, metric.timestamp, metric.value) == ("m1", 1790836200.0, 15.5)


@pytest.mark.parametrize(
    "overrides",
    [{"machine_id": ""}, {"timestamp": "2026-10-01T06:30:00"}, {"timestamp": "yesterday"}, {"metrics": [{"name": "x"}]}],
    ids=["empty-id", "no-offset", "not-iso", "incomplete-metric"],
)
def test_malformed_metrics_raise_a_value_error(overrides):
    with pytest.raises(ValueError):
        MetricsParser.parse(json.dumps({**METRICS_PAYLOAD, **overrides}))


def test_a_body_that_is_not_json_raises_a_value_error():
    with pytest.raises(ValueError):
        MetricsParser.parse("<html>502 Bad Gateway</html>")
