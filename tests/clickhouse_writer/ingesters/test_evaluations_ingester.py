import uuid
from datetime import datetime, timezone

import pytest
from pydantic_core import to_jsonable_python

from activityreporter.clickhouse_writer.ingesters.evaluations_ingester import EvaluationsIngester
from tests.support.builders import METRIC_ID, build_evaluation, parse_record, row_of


def test_evaluation_row_matches_columns_and_keeps_a_missing_band_as_null():
    assert row_of(EvaluationsIngester, build_evaluation(1.5)) == {
        "metric_id": uuid.UUID(METRIC_ID),
        "machine_id": "machine-123",
        "metric_name": "system.cpu.utilization",
        "timestamp": datetime.fromtimestamp(1.5, timezone.utc),
        "algorithm": "ewma",
        "baseline": 21.3,
        "lower": None,
        "upper": 31.3,
        "is_anomaly": True,
    }


def test_evaluation_values_are_stored_exactly_as_sent():
    evaluation = build_evaluation()
    evaluation.baseline = 11.280148426792852

    assert row_of(EvaluationsIngester, evaluation)["baseline"] == 11.280148426792852


@pytest.mark.parametrize("overrides", [{"machine_id": ""}, {"algorithm": ""}, {"baseline": "n/a"}])
def test_parse_skips_invalid_evaluations(overrides):
    assert parse_record(EvaluationsIngester, {**to_jsonable_python(build_evaluation()), **overrides}) is None
