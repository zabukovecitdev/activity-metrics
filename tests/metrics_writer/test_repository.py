import json
import re
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from activityreporter.shared.metrics import ProcessedMetric
from activityreporter.metrics_writer.repository import KafkaProcessedMetricsRepository, TimescaleMetricsRepository


def build_metrics(timestamp: float = 1.0) -> ProcessedMetric:
    return ProcessedMetric(
        timestamp=timestamp,
        name="system.cpu.utilization",
        type="gauge",
        unit="%",
        value=15.0,
        machine_id="machine-123",
        attributes={"core": "0"},
        is_anomaly=False,
    )


def build_connection() -> MagicMock:
    connection = MagicMock(closed=False)
    connection.cursor.return_value.__enter__.return_value = MagicMock()
    return connection


def test_insert_batch_commits_once_on_success():
    connection = build_connection()

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection):
        repository = TimescaleMetricsRepository(dsn="dsn")
        repository.connect()
        repository.insert_batch([build_metrics()])

    cursor = connection.cursor.return_value.__enter__.return_value
    assert cursor.executemany.call_count == 1
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_insert_batch_passes_a_value_for_every_sql_placeholder():
    connection = build_connection()

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection):
        repository = TimescaleMetricsRepository(dsn="dsn")
        repository.connect()
        repository.insert_batch([build_metrics()])

    cursor = connection.cursor.return_value.__enter__.return_value
    sql, rows = cursor.executemany.call_args.args
    placeholders = set(re.findall(r"%\((\w+)\)s", sql))
    assert placeholders == set(rows[0])


def test_insert_batch_stores_missing_attributes_as_empty_object():
    connection = build_connection()
    metric = build_metrics()
    metric.attributes = None

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection):
        repository = TimescaleMetricsRepository(dsn="dsn")
        repository.connect()
        repository.insert_batch([metric])

    cursor = connection.cursor.return_value.__enter__.return_value
    _, rows = cursor.executemany.call_args.args
    assert rows[0]["attributes"].obj == {}


def test_insert_batch_skips_empty_batch():
    connection = build_connection()

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection):
        repository = TimescaleMetricsRepository(dsn="dsn")
        repository.connect()
        repository.insert_batch([])

    assert connection.commit.call_count == 0


def test_insert_batch_retries_with_backoff_then_succeeds():
    connection = build_connection()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = [psycopg.OperationalError("boom"), None]

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection), \
         patch("activityreporter.metrics_writer.repository.time.sleep") as sleep:
        repository = TimescaleMetricsRepository(dsn="dsn", max_retries=3, backoff_base_seconds=1.0)
        repository.connect()
        repository.insert_batch([build_metrics()])

    assert cursor.executemany.call_count == 2
    assert connection.rollback.call_count == 1
    assert connection.commit.call_count == 1
    sleep.assert_called_once_with(1.0)


def test_insert_batch_raises_after_exhausting_retries():
    connection = build_connection()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = psycopg.OperationalError("boom")

    with patch("activityreporter.metrics_writer.repository.psycopg.connect", return_value=connection), \
         patch("activityreporter.metrics_writer.repository.time.sleep") as sleep:
        repository = TimescaleMetricsRepository(dsn="dsn", max_retries=3, backoff_base_seconds=1.0)
        repository.connect()
        with pytest.raises(psycopg.OperationalError):
            repository.insert_batch([build_metrics()])

    assert cursor.executemany.call_count == 3
    assert connection.commit.call_count == 0
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0]


def test_reset_connection_reconnects_when_connection_is_closed():
    closed_connection = MagicMock(closed=True)
    closed_connection.cursor.return_value.__enter__.return_value.executemany.side_effect = (
        psycopg.OperationalError("boom")
    )
    fresh_connection = build_connection()

    with patch(
        "activityreporter.metrics_writer.repository.psycopg.connect",
        side_effect=[closed_connection, fresh_connection],
    ), patch("activityreporter.metrics_writer.repository.time.sleep"):
        repository = TimescaleMetricsRepository(dsn="dsn", max_retries=2)
        repository.connect()
        repository.insert_batch([build_metrics()])

    assert closed_connection.rollback.call_count == 0
    assert fresh_connection.commit.call_count == 1


@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_poll_skips_malformed_records(value):
    valid = MagicMock(value=json.dumps(asdict(build_metrics(1.0))).encode())
    with patch("activityreporter.metrics_writer.repository.KafkaConsumer") as kafka_consumer:
        repository = KafkaProcessedMetricsRepository("localhost:9094", "processed_metrics", "timescale-writer")
    kafka_consumer.return_value.poll.return_value = {"processed_metrics-0": [MagicMock(value=value), valid]}

    assert [m.timestamp for m in repository.poll(max_records=10)] == [1.0]
