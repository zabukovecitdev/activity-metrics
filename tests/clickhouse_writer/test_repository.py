from unittest.mock import MagicMock, patch

import pytest
from clickhouse_connect.driver.exceptions import OperationalError
from kafka.structs import TopicPartition

from activityreporter.clickhouse_writer.repository import ClickHouseRepository, KafkaRecordsRepository

COLUMNS = ["a"]


def insert_with(client: MagicMock, rows: list, **repository_kwargs) -> None:
    with patch("activityreporter.clickhouse_writer.repository.clickhouse_connect.get_client", return_value=client):
        repository = ClickHouseRepository("localhost", 8123, "user", "password", "metrics", **repository_kwargs)
        repository.connect()
        repository.insert_batch("metrics", COLUMNS, rows)


def test_consumer_subscribes_to_its_topic_in_its_group():
    with patch("activityreporter.clickhouse_writer.repository.KafkaConsumer") as kafka_consumer:
        KafkaRecordsRepository("machines", "localhost:9094", "clickhouse-machines-writer")

    assert kafka_consumer.call_args.args == ("machines",)
    assert kafka_consumer.call_args.kwargs["group_id"] == "clickhouse-machines-writer"


def test_poll_returns_the_records_of_every_partition():
    with patch("activityreporter.clickhouse_writer.repository.KafkaConsumer") as kafka_consumer:
        repository = KafkaRecordsRepository("machines", "localhost:9094", "g")
    kafka_consumer.return_value.poll.return_value = {
        TopicPartition("machines", 0): ["r1", "r2"],
        TopicPartition("machines", 1): ["r3"],
    }

    assert repository.poll(max_records=10) == ["r1", "r2", "r3"]


def test_insert_batch_inserts_rows_once_into_the_sink_table():
    client = MagicMock()
    rows = [["row"]]

    insert_with(client, rows)

    client.insert.assert_called_once_with("metrics", rows, column_names=COLUMNS)


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
