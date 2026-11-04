import uuid
from datetime import datetime, timezone

from activityreporter.clickhouse_writer.sinks.clickhouse_metrics_sink import (
    ClickHouseMetricsSink,
)
from tests.support.builders import METRIC_ID, build_metric, row_of


def test_metric_row_matches_columns():
    assert row_of(ClickHouseMetricsSink, build_metric(1.5)) == {
        "metric_id": uuid.UUID(METRIC_ID),
        "machine_id": "machine-123",
        "name": "system.cpu.utilization",
        "timestamp": datetime.fromtimestamp(1.5, timezone.utc),
        "type": "gauge",
        "unit": "%",
        "value": 15.0,
        "attributes": {"core": "0"},
        "pipeline_start": None,
    }


def test_missing_attributes_are_stored_as_an_empty_map():
    metric = build_metric()
    metric.attributes = None

    assert row_of(ClickHouseMetricsSink, metric)["attributes"] == {}


def test_pipeline_start_is_stored_as_utc():
    metric = build_metric()
    metric.pipeline_start = 2.5

    assert row_of(ClickHouseMetricsSink, metric)[
        "pipeline_start"
    ] == datetime.fromtimestamp(2.5, timezone.utc)
