from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer.repository import EVALUATIONS, METRICS, SINKS
from activityreporter.clickhouse_writer.service import ClickHouseWriter


class LoopBreak(Exception):
    pass


def build_writer(batch_size=100, batch_timeout_seconds=5.0):
    records, store = MagicMock(), MagicMock()
    writer = ClickHouseWriter(records, store, SINKS, batch_size, batch_timeout_seconds)
    return writer, records, store


def inserted(store) -> dict:
    """Rows passed to insert_batch, by table, in call order."""
    result: dict = {}
    for call in store.insert_batch.call_args_list:
        sink, rows = call.args
        result.setdefault(sink.table, []).extend(rows)
    return result


def test_run_flushes_when_one_table_reaches_the_batch_size():
    writer, records, store = build_writer(batch_size=2)
    records.poll.side_effect = [{"metrics": [["m1"]]}, {"metrics": [["m2"]]}, LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    assert inserted(store) == {"metrics": [["m1"], ["m2"]]}
    assert records.commit.call_count == 1


def test_flush_writes_every_pending_table_with_its_own_sink():
    writer, records, store = build_writer(batch_size=2)
    records.poll.side_effect = [{"evaluations": [["e1"]], "metrics": [["m1"], ["m2"]]}, LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    sinks = [call.args[0] for call in store.insert_batch.call_args_list]
    assert sorted(sinks, key=lambda sink: sink.table) == [EVALUATIONS, METRICS]
    assert inserted(store) == {"evaluations": [["e1"]], "metrics": [["m1"], ["m2"]]}
    assert records.commit.call_count == 1


def test_run_flushes_on_timeout_before_batch_is_full():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=5.0)
    records.poll.side_effect = [{"machines": [["h1"]]}, LoopBreak]

    with patch("activityreporter.clickhouse_writer.service.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        writer.run()

    assert inserted(store) == {"machines": [["h1"]]}
    assert records.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    writer, records, store = build_writer(batch_size=1)
    records.poll.side_effect = [{}, LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_stop_flushes_pending_batch():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        writer.stop()
        return {"metrics": [["m1"]]}

    records.poll.side_effect = poll_then_stop

    writer.run()

    assert inserted(store) == {"metrics": [["m1"]]}
    assert records.commit.call_count == 1


def test_flush_does_not_commit_offsets_when_any_table_fails():
    writer, records, store = build_writer()
    store.insert_batch.side_effect = [None, RuntimeError("db is down")]

    with pytest.raises(RuntimeError):
        writer._flush({"metrics": [["m1"]], "evaluations": [["e1"]]})

    records.commit.assert_not_called()


def test_flush_commits_offsets_only_after_every_write():
    writer, records, store = build_writer()
    call_order = []
    store.insert_batch.side_effect = lambda sink, rows: call_order.append(sink.table)
    records.commit.side_effect = lambda: call_order.append("commit")

    writer._flush({"metrics": [["m1"]], "machines": [["h1"]]})

    assert call_order == ["metrics", "machines", "commit"]
