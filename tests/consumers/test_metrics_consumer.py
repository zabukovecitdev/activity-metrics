from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from client.metric_factory import Metrics
from consumers.metrics_consumer import MetricsConsumer


class LoopBreak(Exception):
    """Raised by a mocked poll() to terminate the consumer's infinite run loop."""


def build_metrics(timestamp: float) -> Metrics:
    return Metrics(
        timestamp=timestamp,
        cpu_usage=15.0,
        memory_usage=50.0,
        memory_total=100.0,
        labels={"name": "test"},
        machine_id="machine-123",
        battery_charging=True,
        battery_percentage=80.0,
    )


def poll_result(*timestamps: float) -> dict:
    records = [MagicMock(value=asdict(build_metrics(ts))) for ts in timestamps]
    return {"metrics-0": records}


def build_consumer(writer, batch_size=100, batch_timeout_seconds=5.0):
    with patch("consumers.metrics_consumer.KafkaConsumer") as kafka_consumer:
        consumer = MetricsConsumer(
            bootstrap_servers="localhost:9094",
            topic="metrics",
            group_id="timescale-writer",
            writer=writer,
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout_seconds,
        )
    return consumer, kafka_consumer.return_value


def test_run_flushes_when_batch_size_is_reached():
    writer = MagicMock()
    consumer, kafka = build_consumer(writer, batch_size=2)
    kafka.poll.side_effect = [poll_result(1.0), poll_result(2.0), LoopBreak]

    with pytest.raises(LoopBreak):
        consumer.run()

    writer.insert_batch.assert_called_once()
    flushed = writer.insert_batch.call_args.args[0]
    assert [m.timestamp for m in flushed] == [1.0, 2.0]
    assert kafka.commit.call_count == 1
    kafka.close.assert_called_once()


def test_run_flushes_on_timeout_before_batch_is_full():
    writer = MagicMock()
    consumer, kafka = build_consumer(writer, batch_size=100, batch_timeout_seconds=5.0)
    kafka.poll.side_effect = [poll_result(1.0), LoopBreak]

    # Second monotonic() reading is past the batch timeout relative to the first.
    with patch("consumers.metrics_consumer.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        consumer.run()

    writer.insert_batch.assert_called_once()
    assert len(writer.insert_batch.call_args.args[0]) == 1
    assert kafka.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    writer = MagicMock()
    consumer, kafka = build_consumer(writer, batch_size=1)
    kafka.poll.side_effect = [{}, LoopBreak]

    with pytest.raises(LoopBreak):
        consumer.run()

    writer.insert_batch.assert_not_called()
    kafka.commit.assert_not_called()


def test_stop_flushes_pending_batch_and_closes_consumer():
    writer = MagicMock()
    consumer, kafka = build_consumer(writer, batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        consumer.stop()
        return poll_result(1.0)

    kafka.poll.side_effect = poll_then_stop

    consumer.run()

    writer.insert_batch.assert_called_once()
    assert len(writer.insert_batch.call_args.args[0]) == 1
    assert kafka.commit.call_count == 1
    kafka.close.assert_called_once()


def test_flush_does_not_commit_offsets_when_write_fails():
    writer = MagicMock()
    writer.insert_batch.side_effect = RuntimeError("db is down")
    consumer, kafka = build_consumer(writer)

    with pytest.raises(RuntimeError):
        consumer._flush([build_metrics(1.0)])

    kafka.commit.assert_not_called()


def test_flush_commits_offsets_only_after_successful_write():
    writer = MagicMock()
    consumer, kafka = build_consumer(writer)
    call_order = []
    writer.insert_batch.side_effect = lambda batch: call_order.append("insert")
    kafka.commit.side_effect = lambda: call_order.append("commit")

    consumer._flush([build_metrics(1.0)])

    assert call_order == ["insert", "commit"]
