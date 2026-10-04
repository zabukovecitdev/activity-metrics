import json
import uuid
from dataclasses import asdict, fields
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from kafka.structs import TopicPartition

from activityreporter.clickhouse_writer.repository import (
    DEFAULT_GROUP_ID,
    EVALUATIONS,
    MACHINES,
    METRICS,
    SINKS as ALL_SINKS,
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


SINKS = pytest.mark.parametrize(
    "sink, build",
    [(METRICS, build_metric), (EVALUATIONS, build_evaluation), (MACHINES, build_machine)],
    ids=["metrics", "evaluations", "machines"],
)


def kafka_record(payload) -> MagicMock:
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return MagicMock(value=value)


def row_of(sink, record) -> dict:
    row = parse_record(sink, kafka_record(asdict(record)))
    return dict(zip(sink.columns, row, strict=True))


def insert_with(client: MagicMock, rows: list, **repository_kwargs) -> None:
    with patch("activityreporter.clickhouse_writer.repository.clickhouse_connect.get_client", return_value=client):
        repository = ClickHouseRepository("localhost", 8123, "user", "password", "metrics", **repository_kwargs)
        repository.connect()
        repository.insert_batch(METRICS, rows)


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


def test_evaluation_row_matches_columns_and_keeps_a_missing_band_as_null():
    assert row_of(EVALUATIONS, build_evaluation(1.5)) == {
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

    assert row_of(EVALUATIONS, evaluation)["baseline"] == 11.280148426792852


@pytest.mark.parametrize("overrides", [{"machine_id": ""}, {"algorithm": ""}, {"baseline": "n/a"}])
def test_parse_record_skips_invalid_evaluations(overrides):
    payload = {**asdict(build_evaluation()), **overrides}

    assert parse_record(EVALUATIONS, kafka_record(payload)) is None


def test_missing_attributes_are_stored_as_an_empty_map():
    metric = build_metric()
    metric.attributes = None

    assert row_of(METRICS, metric)["attributes"] == {}


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


@pytest.mark.parametrize(
    "sink, build", [(METRICS, build_metric), (EVALUATIONS, build_evaluation)], ids=["metrics", "evaluations"],
)
@pytest.mark.parametrize("metric_id", [None, "not-a-uuid", 5])
def test_parse_record_skips_records_without_a_valid_metric_id(sink, build, metric_id):
    payload = {**asdict(build()), "metric_id": metric_id}

    assert parse_record(sink, kafka_record(payload)) is None


def build_consumer(sinks_by_topic=None):
    with patch("activityreporter.clickhouse_writer.repository.KafkaConsumer") as kafka_consumer:
        repository = KafkaRecordsRepository(
            sinks_by_topic or {sink.default_topic: sink for sink in ALL_SINKS}, "localhost:9094", DEFAULT_GROUP_ID
        )
    return repository, kafka_consumer


def test_one_consumer_subscribes_to_every_sink_topic():
    _, kafka_consumer = build_consumer()

    assert set(kafka_consumer.call_args.args) == {"raw_metrics", "evaluations", "machines"}
    assert kafka_consumer.call_args.kwargs["group_id"] == DEFAULT_GROUP_ID


def test_sink_topics_follow_the_producers_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_EVALUATIONS_TOPIC", "scores")

    assert EVALUATIONS.topic() == "scores"
    assert METRICS.topic() == "raw_metrics"


def test_poll_groups_rows_by_table_using_each_topics_sink():
    repository, kafka_consumer = build_consumer()
    metric, evaluation = kafka_record(asdict(build_metric())), kafka_record(asdict(build_evaluation()))
    kafka_consumer.return_value.poll.return_value = {
        TopicPartition("raw_metrics", 0): [metric],
        TopicPartition("evaluations", 0): [evaluation],
    }

    rows = repository.poll(max_records=10)

    assert rows == {"metrics": [parse_record(METRICS, metric)], "evaluations": [parse_record(EVALUATIONS, evaluation)]}


@SINKS
@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_poll_skips_malformed_records(sink, build, value):
    repository, kafka_consumer = build_consumer()
    valid = kafka_record(asdict(build(1.0)))
    kafka_consumer.return_value.poll.return_value = {
        TopicPartition(sink.default_topic, 0): [kafka_record(value), valid],
    }

    rows = repository.poll(max_records=10)

    assert rows == {sink.table: [parse_record(sink, valid)]}


def test_poll_leaves_out_tables_with_only_malformed_records():
    repository, kafka_consumer = build_consumer()
    kafka_consumer.return_value.poll.return_value = {TopicPartition("machines", 0): [kafka_record(b"not json")]}

    assert repository.poll(max_records=10) == {}


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
