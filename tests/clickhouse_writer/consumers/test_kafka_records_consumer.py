from unittest.mock import patch

from kafka.structs import TopicPartition

from activityreporter.clickhouse_writer.consumers.kafka_records_consumer import KafkaRecordsConsumer


def test_consumer_subscribes_to_its_topic_in_its_group():
    with patch("activityreporter.clickhouse_writer.consumers.kafka_records_consumer.KafkaConsumer") as kafka_consumer:
        KafkaRecordsConsumer("machines", "localhost:9094", "clickhouse-machines-writer")

    assert kafka_consumer.call_args.args == ("machines",)
    assert kafka_consumer.call_args.kwargs["group_id"] == "clickhouse-machines-writer"


def test_poll_returns_the_records_of_every_partition():
    with patch("activityreporter.clickhouse_writer.consumers.kafka_records_consumer.KafkaConsumer") as kafka_consumer:
        consumer = KafkaRecordsConsumer("machines", "localhost:9094", "g")
    kafka_consumer.return_value.poll.return_value = {
        TopicPartition("machines", 0): ["r1", "r2"],
        TopicPartition("machines", 1): ["r3"],
    }

    assert consumer.poll(max_records=10) == ["r1", "r2", "r3"]
