import json
from dataclasses import asdict
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from activityreporter.shared.metrics import ProcessedMetric
from activityreporter.metrics_writer.repository import (
    METRICS_COLUMNS,
    ClickHouseMetricsRepository,
    KafkaProcessedMetricsRepository,
)


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


def insert_with(client: MagicMock, metrics: list[ProcessedMetric], **repository_kwargs) -> None:
    with patch("activityreporter.metrics_writer.repository.clickhouse_connect.get_client", return_value=client):
        repository = ClickHouseMetricsRepository("localhost", 8123, "user", "password", "metrics", **repository_kwargs)
        repository.connect()
        repository.insert_batch(metrics)


def inserted_rows(client: MagicMock) -> list[dict]:
    _, rows = client.insert.call_args.args
    column_names = client.insert.call_args.kwargs["column_names"]
    return [dict(zip(column_names, row, strict=True)) for row in rows]


def test_insert_batch_inserts_once_on_success():
    client = MagicMock()

    insert_with(client, [build_metrics()])

    assert client.insert.call_count == 1
    assert client.insert.call_args.args[0] == "metrics"


def test_insert_batch_writes_a_value_for_every_metric_column():
    client = MagicMock()

    insert_with(client, [build_metrics(1.5)])

    assert inserted_rows(client) == [{
        "machine_id": "machine-123",
        "name": "system.cpu.utilization",
        "timestamp": datetime.fromtimestamp(1.5, timezone.utc),
        "type": "gauge",
        "unit": "%",
        "value": 15.0,
        "attributes": {"core": "0"},
        "is_anomaly": False,
    }]
    assert set(METRICS_COLUMNS) == set(asdict(build_metrics()))


def test_insert_batch_stores_missing_attributes_as_empty_map():
    client = MagicMock()
    metric = build_metrics()
    metric.attributes = None

    insert_with(client, [metric])

    assert inserted_rows(client)[0]["attributes"] == {}


def test_insert_batch_skips_empty_batch():
    client = MagicMock()

    insert_with(client, [])

    client.insert.assert_not_called()


def test_insert_batch_retries_with_backoff_then_succeeds():
    client = MagicMock()
    client.insert.side_effect = [OperationalError("boom"), None]

    with patch("activityreporter.metrics_writer.repository.time.sleep") as sleep:
        insert_with(client, [build_metrics()], max_retries=3, backoff_base_seconds=1.0)

    assert client.insert.call_count == 2
    sleep.assert_called_once_with(1.0)


def test_insert_batch_raises_after_exhausting_retries():
    client = MagicMock()
    client.insert.side_effect = OperationalError("boom")

    with patch("activityreporter.metrics_writer.repository.time.sleep") as sleep, \
         pytest.raises(OperationalError):
        insert_with(client, [build_metrics()], max_retries=3, backoff_base_seconds=1.0)

    assert client.insert.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0]


@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_poll_skips_malformed_records(value):
    valid = MagicMock(value=json.dumps(asdict(build_metrics(1.0))).encode())
    with patch("activityreporter.metrics_writer.repository.KafkaConsumer") as kafka_consumer:
        repository = KafkaProcessedMetricsRepository("localhost:9094", "processed_metrics", "clickhouse-writer")
    kafka_consumer.return_value.poll.return_value = {"processed_metrics-0": [MagicMock(value=value), valid]}

    assert [m.timestamp for m in repository.poll(max_records=10)] == [1.0]
