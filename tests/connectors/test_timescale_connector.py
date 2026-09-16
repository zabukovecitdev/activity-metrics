from unittest.mock import MagicMock, patch

import psycopg
import pytest

from client.metric_factory import Metrics
from connectors.timescale_connector import TimescaleConnector


def build_metrics(timestamp: float = 1.0) -> Metrics:
    return Metrics(
        timestamp=timestamp,
        boot_time=1000.0,
        core_count=2,
        cpu_usage=[10.0, 20.0],
        memory_usage=50.0,
        memory_total=100.0,
        labels={"name": "test"},
        machine_id="machine-123",
    )


def build_connection() -> MagicMock:
    connection = MagicMock(closed=False)
    connection.cursor.return_value.__enter__.return_value = MagicMock()
    return connection


def test_insert_batch_commits_once_on_success():
    connection = build_connection()

    with patch("connectors.timescale_connector.psycopg.connect", return_value=connection):
        connector = TimescaleConnector(dsn="dsn")
        connector.connect()
        connector.insert_batch([build_metrics()])

    cursor = connection.cursor.return_value.__enter__.return_value
    assert cursor.executemany.call_count == 1
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_insert_batch_skips_empty_batch():
    connection = build_connection()

    with patch("connectors.timescale_connector.psycopg.connect", return_value=connection):
        connector = TimescaleConnector(dsn="dsn")
        connector.connect()
        connector.insert_batch([])

    assert connection.commit.call_count == 0


def test_insert_batch_retries_with_backoff_then_succeeds():
    connection = build_connection()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = [psycopg.OperationalError("boom"), None]

    with patch("connectors.timescale_connector.psycopg.connect", return_value=connection), \
         patch("connectors.timescale_connector.time.sleep") as sleep:
        connector = TimescaleConnector(dsn="dsn", max_retries=3, backoff_base_seconds=1.0)
        connector.connect()
        connector.insert_batch([build_metrics()])

    assert cursor.executemany.call_count == 2
    assert connection.rollback.call_count == 1
    assert connection.commit.call_count == 1
    sleep.assert_called_once_with(1.0)


def test_insert_batch_raises_after_exhausting_retries():
    connection = build_connection()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = psycopg.OperationalError("boom")

    with patch("connectors.timescale_connector.psycopg.connect", return_value=connection), \
         patch("connectors.timescale_connector.time.sleep") as sleep:
        connector = TimescaleConnector(dsn="dsn", max_retries=3, backoff_base_seconds=1.0)
        connector.connect()
        with pytest.raises(psycopg.OperationalError):
            connector.insert_batch([build_metrics()])

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
        "connectors.timescale_connector.psycopg.connect",
        side_effect=[closed_connection, fresh_connection],
    ), patch("connectors.timescale_connector.time.sleep"):
        connector = TimescaleConnector(dsn="dsn", max_retries=2)
        connector.connect()
        connector.insert_batch([build_metrics()])

    assert closed_connection.rollback.call_count == 0
    assert fresh_connection.commit.call_count == 1
