from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer.service import ClickHouseWriter
from activityreporter.shared.metrics import Metric


class LoopBreak(Exception):
    pass


def build_metric(timestamp: float) -> Metric:
    return Metric(
        timestamp=timestamp,
        name="system.cpu.utilization",
        type="gauge",
        unit="%",
        value=15.0,
        machine_id="machine-123",
        attributes={"core": "0"},
    )


def build_writer(batch_size=100, batch_timeout_seconds=5.0):
    records, store = MagicMock(), MagicMock()
    writer = ClickHouseWriter(records, store, batch_size, batch_timeout_seconds)
    return writer, records, store


def test_run_flushes_when_batch_size_is_reached():
    writer, records, store = build_writer(batch_size=2)
    records.poll.side_effect = [[build_metric(1.0)], [build_metric(2.0)], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_called_once()
    assert [m.timestamp for m in store.insert_batch.call_args.args[0]] == [1.0, 2.0]
    assert records.commit.call_count == 1


def test_run_flushes_on_timeout_before_batch_is_full():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=5.0)
    records.poll.side_effect = [[build_metric(1.0)], LoopBreak]

    with patch("activityreporter.clickhouse_writer.service.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_called_once()
    assert len(store.insert_batch.call_args.args[0]) == 1
    assert records.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    writer, records, store = build_writer(batch_size=1)
    records.poll.side_effect = [[], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_stop_flushes_pending_batch():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        writer.stop()
        return [build_metric(1.0)]

    records.poll.side_effect = poll_then_stop

    writer.run()

    store.insert_batch.assert_called_once()
    assert len(store.insert_batch.call_args.args[0]) == 1
    assert records.commit.call_count == 1


def test_flush_does_not_commit_offsets_when_write_fails():
    writer, records, store = build_writer()
    store.insert_batch.side_effect = RuntimeError("db is down")

    with pytest.raises(RuntimeError):
        writer._flush([build_metric(1.0)])

    records.commit.assert_not_called()


def test_flush_commits_offsets_only_after_successful_write():
    writer, records, store = build_writer()
    call_order = []
    store.insert_batch.side_effect = lambda batch: call_order.append("insert")
    records.commit.side_effect = lambda: call_order.append("commit")

    writer._flush([build_metric(1.0)])

    assert call_order == ["insert", "commit"]
