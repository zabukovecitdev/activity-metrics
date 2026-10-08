import json
import uuid
from dataclasses import asdict, fields
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from activityreporter.clickhouse_writer.service import TopicWriter
from activityreporter.clickhouse_writer.writers import WRITERS, EvaluationsWriter, MachinesWriter, MetricsWriter
from activityreporter.shared.models import Evaluation, Machine, Metric
from activityreporter.shared.settings import KafkaSettings

METRIC_ID = "01a0e974-dc94-74df-ae87-e0e6064b18ef"


def build_metric(timestamp: float = 1.0) -> Metric:
    return Metric(
        timestamp=timestamp,
        name="system.cpu.utilization",
        type="gauge",
        unit="%",
        value=15.0,
        machine_id="machine-123",
        metric_id=METRIC_ID,
        attributes={"core": "0"},
    )


def build_evaluation(timestamp: float = 1.0) -> Evaluation:
    return Evaluation(
        metric_id=METRIC_ID,
        machine_id="machine-123",
        metric_name="system.cpu.utilization",
        timestamp=timestamp,
        algorithm="ewma",
        baseline=21.3,
        lower=None,
        upper=31.3,
        is_anomaly=True,
    )


def build_machine(timestamp: float = 1.0) -> Machine:
    return Machine(
        machine_id="machine-123",
        hostname="workstation",
        os="linux",
        os_version="6.18.0",
        architecture="x86_64",
        cores=16,
        total_memory=34_359_738_368,
        total_disk=1_000_204_886_016,
        last_boot="2026-10-01T06:30:00+00:00",
        observed_at=timestamp,
    )


WRITER_CASES = pytest.mark.parametrize(
    "writer_type, build",
    [(MetricsWriter, build_metric), (EvaluationsWriter, build_evaluation), (MachinesWriter, build_machine)],
    ids=["metrics", "evaluations", "machines"],
)


def kafka_record(payload) -> MagicMock:
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return MagicMock(value=value)


def parse(writer_type, payload):
    return writer_type(MagicMock(), MagicMock())._parse(kafka_record(payload))


def row_of(writer_type, record) -> dict:
    return dict(zip(writer_type.columns, parse(writer_type, asdict(record)), strict=True))


def test_topic_writer_is_abstract():
    with pytest.raises(TypeError):
        TopicWriter(MagicMock(), MagicMock())


@WRITER_CASES
def test_columns_are_the_record_fields(writer_type, build):
    assert set(writer_type.columns) == {f.name for f in fields(writer_type.record_type)}


def test_each_writer_has_its_own_group():
    assert sorted(w.group_id() for w in WRITERS) == [
        "clickhouse-evaluations-writer", "clickhouse-machines-writer", "clickhouse-metrics-writer",
    ]


def test_topics_follow_the_producers_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_EVALUATIONS_TOPIC", "scores")

    kafka = KafkaSettings()

    assert EvaluationsWriter.topic(kafka) == "scores"
    assert MetricsWriter.topic(kafka) == "raw_metrics"
    assert MachinesWriter.topic(kafka) == "machines"


def test_metric_row_matches_columns():
    assert row_of(MetricsWriter, build_metric(1.5)) == {
        "metric_id": uuid.UUID(METRIC_ID),
        "machine_id": "machine-123",
        "name": "system.cpu.utilization",
        "timestamp": datetime.fromtimestamp(1.5, timezone.utc),
        "type": "gauge",
        "unit": "%",
        "value": 15.0,
        "attributes": {"core": "0"},
    }


def test_evaluation_row_matches_columns_and_keeps_a_missing_band_as_null():
    assert row_of(EvaluationsWriter, build_evaluation(1.5)) == {
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

    assert row_of(EvaluationsWriter, evaluation)["baseline"] == 11.280148426792852


@pytest.mark.parametrize("overrides", [{"machine_id": ""}, {"algorithm": ""}, {"baseline": "n/a"}])
def test_parse_skips_invalid_evaluations(overrides):
    assert parse(EvaluationsWriter, {**asdict(build_evaluation()), **overrides}) is None


def test_missing_attributes_are_stored_as_an_empty_map():
    metric = build_metric()
    metric.attributes = None

    assert row_of(MetricsWriter, metric)["attributes"] == {}


def test_machine_row_converts_both_times_to_utc():
    row = row_of(MachinesWriter, build_machine(1.5))

    assert row["last_boot"] == datetime(2026, 10, 1, 6, 30, tzinfo=timezone.utc)
    assert row["observed_at"] == datetime.fromtimestamp(1.5, timezone.utc)
    assert (row["cores"], row["total_memory"]) == (16, 34_359_738_368)


@pytest.mark.parametrize(
    "overrides",
    [{"machine_id": ""}, {"last_boot": "yesterday"}, {"last_boot": "2026-10-01T06:30:00"}],
    ids=["empty-id", "not-iso", "no-offset"],
)
def test_parse_skips_invalid_machines(overrides):
    assert parse(MachinesWriter, {**asdict(build_machine()), **overrides}) is None


@WRITER_CASES
def test_parse_ignores_unknown_fields(writer_type, build):
    assert parse(writer_type, {**asdict(build()), "added_later": "x"}) == parse(writer_type, asdict(build()))


@pytest.mark.parametrize(
    "writer_type, build", [(MetricsWriter, build_metric), (EvaluationsWriter, build_evaluation)],
    ids=["metrics", "evaluations"],
)
@pytest.mark.parametrize("metric_id", [None, "not-a-uuid", 5])
def test_parse_skips_records_without_a_valid_metric_id(writer_type, build, metric_id):
    assert parse(writer_type, {**asdict(build()), "metric_id": metric_id}) is None


@WRITER_CASES
@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_parse_skips_malformed_records(writer_type, build, value):
    assert parse(writer_type, value) is None
