from unittest.mock import MagicMock, patch

import pytest

from activityreporter.metrics_writer.service import MetricsWriter
from activityreporter.shared.metrics import ProcessedMetric


class LoopBreak(Exception):
    pass


def build_metric(timestamp: float) -> ProcessedMetric:
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


def build_writer(batch_size=100, batch_timeout_seconds=5.0):
    processed_metrics, timescale_metrics = MagicMock(), MagicMock()
    writer = MetricsWriter(processed_metrics, timescale_metrics, batch_size, batch_timeout_seconds)
    return writer, processed_metrics, timescale_metrics


def test_run_flushes_when_batch_size_is_reached():
    writer, processed, timescale = build_writer(batch_size=2)
    processed.poll.side_effect = [[build_metric(1.0)], [build_metric(2.0)], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    timescale.insert_batch.assert_called_once()
    assert [m.timestamp for m in timescale.insert_batch.call_args.args[0]] == [1.0, 2.0]
    assert processed.commit.call_count == 1


def test_run_flushes_on_timeout_before_batch_is_full():
    writer, processed, timescale = build_writer(batch_size=100, batch_timeout_seconds=5.0)
    processed.poll.side_effect = [[build_metric(1.0)], LoopBreak]

    with patch("activityreporter.metrics_writer.service.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        writer.run()

    timescale.insert_batch.assert_called_once()
    assert len(timescale.insert_batch.call_args.args[0]) == 1
    assert processed.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    writer, processed, timescale = build_writer(batch_size=1)
    processed.poll.side_effect = [[], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    timescale.insert_batch.assert_not_called()
    processed.commit.assert_not_called()


def test_stop_flushes_pending_batch():
    writer, processed, timescale = build_writer(batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        writer.stop()
        return [build_metric(1.0)]

    processed.poll.side_effect = poll_then_stop

    writer.run()

    timescale.insert_batch.assert_called_once()
    assert len(timescale.insert_batch.call_args.args[0]) == 1
    assert processed.commit.call_count == 1


def test_flush_does_not_commit_offsets_when_write_fails():
    writer, processed, timescale = build_writer()
    timescale.insert_batch.side_effect = RuntimeError("db is down")

    with pytest.raises(RuntimeError):
        writer._flush([build_metric(1.0)])

    processed.commit.assert_not_called()


def test_flush_commits_offsets_only_after_successful_write():
    writer, processed, timescale = build_writer()
    call_order = []
    timescale.insert_batch.side_effect = lambda batch: call_order.append("insert")
    processed.commit.side_effect = lambda: call_order.append("commit")

    writer._flush([build_metric(1.0)])

    assert call_order == ["insert", "commit"]
