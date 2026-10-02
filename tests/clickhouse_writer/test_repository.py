import json
import uuid
from dataclasses import asdict, fields
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from activityreporter.clickhouse_writer.repository import (
    EVALUATIONS,
    MACHINES,
    METRICS,
    ClickHouseRepository,
    KafkaRecordsRepository,
    parse_record,
)
from activityreporter.shared.evaluations import Evaluation
from activityreporter.shared.machines import Machine
from activityreporter.shared.metrics import Metric

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
        value=87.4,
        algorithm="ewma",
        algorithm_version=1,
        params={"alpha": 0.1, "threshold": 1.0, "window_ms": 3_600_000.0},
        baseline=21.3,
        lower=None,
        upper=31.3,
        score=6.6,
        threshold=1.0,
        is_anomaly=True,
        direction=1,
        details={"scale": 10.0, "window_size": 342.0},
        detected_at=2.5,
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


SINKS = pytest.mark.parametrize(
    "sink, build",
    [(METRICS, build_metric), (EVALUATIONS, build_evaluation), (MACHINES, build_machine)],
    ids=["metrics", "evaluations", "machines"],
)
SAMPLE_SINKS = pytest.mark.parametrize(
    "sink, build",
    [(METRICS, build_metric), (EVALUATIONS, build_evaluation)],
    ids=["metrics", "evaluations"],
)


def kafka_record(payload) -> MagicMock:
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return MagicMock(value=value)


def row_of(sink, record) -> dict:
    row = parse_record(sink, kafka_record(asdict(record)))
    return dict(zip(sink.columns, row, strict=True))


def insert_with(client: MagicMock, rows: list, **repository_kwargs) -> None:
    with patch("activityreporter.clickhouse_writer.repository.clickhouse_connect.get_client", return_value=client):
        repository = ClickHouseRepository(METRICS, "localhost", 8123, "user", "password", "metrics", **repository_kwargs)
        repository.connect()
        repository.insert_batch(rows)


@SINKS
def test_sink_columns_are_the_record_fields(sink, build):
    assert set(sink.columns) == {f.name for f in fields(sink.record_type)}


def test_metric_row_matches_columns():
    assert row_of(METRICS, build_metric(1.5)) == {
        "metric_id": uuid.UUID(METRIC_ID),
        "machine_id": "machine-123",
        "name": "system.cpu.utilization",
        "timestamp": datetime.fromtimestamp(1.5, timezone.utc),
        "type": "gauge",
        "unit": "%",
        "value": 15.0,
        "attributes": {"core": "0"},
    }


def test_evaluation_row_converts_id_and_both_timestamps():
    row = row_of(EVALUATIONS, build_evaluation(1.5))

    assert row["metric_id"] == uuid.UUID(METRIC_ID)
    assert row["timestamp"] == datetime.fromtimestamp(1.5, timezone.utc)
    assert row["detected_at"] == datetime.fromtimestamp(2.5, timezone.utc)
    assert row["schema_version"] == 1


def test_evaluation_row_keeps_missing_bands_as_null():
    row = row_of(EVALUATIONS, build_evaluation())

    assert (row["baseline"], row["lower"], row["upper"]) == (21.3, None, 31.3)
    assert row["is_anomaly"] is True
    assert row["params"] == {"alpha": 0.1, "threshold": 1.0, "window_ms": 3_600_000.0}


def test_missing_maps_are_stored_as_empty_maps():
    metric = build_metric()
    metric.attributes = None
    evaluation = build_evaluation()
    evaluation.params = None
    evaluation.details = None

    assert row_of(METRICS, metric)["attributes"] == {}
    row = row_of(EVALUATIONS, evaluation)
    assert row["params"] == {}
    assert row["details"] == {}


def test_machine_row_converts_both_times_to_utc():
    row = row_of(MACHINES, build_machine(1.5))

    assert row["last_boot"] == datetime(2026, 10, 1, 6, 30, tzinfo=timezone.utc)
    assert row["observed_at"] == datetime.fromtimestamp(1.5, timezone.utc)
    assert (row["cores"], row["total_memory"]) == (16, 34_359_738_368)


@pytest.mark.parametrize(
    "overrides",
    [{"machine_id": ""}, {"last_boot": "yesterday"}, {"last_boot": "2026-10-01T06:30:00"}],
    ids=["empty-id", "not-iso", "no-offset"],
)
def test_parse_record_skips_invalid_machines(overrides):
    payload = {**asdict(build_machine()), **overrides}

    assert parse_record(MACHINES, kafka_record(payload)) is None


@SINKS
def test_parse_record_ignores_unknown_fields(sink, build):
    payload = {**asdict(build()), "added_later": "x"}

    assert parse_record(sink, kafka_record(payload)) == parse_record(sink, kafka_record(asdict(build())))


@SAMPLE_SINKS
@pytest.mark.parametrize("metric_id", [None, "not-a-uuid", 5])
def test_parse_record_skips_records_without_a_valid_metric_id(sink, build, metric_id):
    payload = {**asdict(build()), "metric_id": metric_id}

    assert parse_record(sink, kafka_record(payload)) is None


@SINKS
@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_poll_skips_malformed_records(sink, build, value):
    valid = kafka_record(asdict(build(1.0)))
    with patch("activityreporter.clickhouse_writer.repository.KafkaConsumer") as kafka_consumer:
        repository = KafkaRecordsRepository(sink, "localhost:9094", sink.default_topic, sink.default_group_id)
    kafka_consumer.return_value.poll.return_value = {f"{sink.default_topic}-0": [kafka_record(value), valid]}

    rows = repository.poll(max_records=10)

    assert rows == [parse_record(sink, valid)]


def test_insert_batch_inserts_rows_once_into_the_sink_table():
    client = MagicMock()
    rows = [["row"]]

    insert_with(client, rows)

    client.insert.assert_called_once_with("metrics", rows, column_names=METRICS.columns)


def test_insert_batch_skips_empty_batch():
    client = MagicMock()

    insert_with(client, [])

    client.insert.assert_not_called()


def test_insert_batch_retries_with_backoff_then_succeeds():
    client = MagicMock()
    client.insert.side_effect = [OperationalError("boom"), None]

    with patch("activityreporter.clickhouse_writer.repository.time.sleep") as sleep:
        insert_with(client, [["row"]], max_retries=3, backoff_base_seconds=1.0)

    assert client.insert.call_count == 2
    sleep.assert_called_once_with(1.0)


def test_insert_batch_raises_after_exhausting_retries():
    client = MagicMock()
    client.insert.side_effect = OperationalError("boom")

    with patch("activityreporter.clickhouse_writer.repository.time.sleep") as sleep, \
         pytest.raises(OperationalError):
        insert_with(client, [["row"]], max_retries=3, backoff_base_seconds=1.0)

    assert client.insert.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0]
