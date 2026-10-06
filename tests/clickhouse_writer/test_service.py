from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer.writers import MachinesWriter
from tests.clickhouse_writer.test_writers import build_machine, kafka_record


class LoopBreak(Exception):
    pass


def machine_record(machine_id: str) -> MagicMock:
    return kafka_record({**asdict(build_machine()), "machine_id": machine_id})


def machine_row(machine_id: str) -> list:
    return MachinesWriter(MagicMock(), MagicMock())._parse(machine_record(machine_id))


def build_writer(batch_size=100, batch_timeout_seconds=5.0):
    records, store = MagicMock(), MagicMock()
    return MachinesWriter(records, store, batch_size, batch_timeout_seconds), records, store


def inserted(store) -> list:
    """Rows passed to insert_batch, in call order."""
    return [row for call in store.insert_batch.call_args_list for row in call.args[2]]


def test_run_flushes_when_the_batch_is_full():
    writer, records, store = build_writer(batch_size=2)
    records.poll.side_effect = [[machine_record("h1")], [machine_record("h2")], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    assert inserted(store) == [machine_row("h1"), machine_row("h2")]
    assert store.insert_batch.call_args.args[:2] == ("machines", MachinesWriter.columns)
    assert records.commit.call_count == 1


def test_run_flushes_on_timeout_before_batch_is_full():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=5.0)
    records.poll.side_effect = [[machine_record("h1")], LoopBreak]

    with patch("activityreporter.clickhouse_writer.service.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        writer.run()

    assert inserted(store) == [machine_row("h1")]
    assert records.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    writer, records, store = build_writer(batch_size=1)
    records.poll.side_effect = [[], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_run_does_not_flush_when_every_record_is_malformed():
    writer, records, store = build_writer(batch_size=1)
    records.poll.side_effect = [[kafka_record(b"not json")], LoopBreak]

    with pytest.raises(LoopBreak):
        writer.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_stop_flushes_pending_batch():
    writer, records, store = build_writer(batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        writer.stop()
        return [machine_record("h1")]

    records.poll.side_effect = poll_then_stop

    writer.run()

    assert inserted(store) == [machine_row("h1")]
    assert records.commit.call_count == 1


def test_flush_does_not_commit_offsets_when_the_write_fails():
    writer, records, store = build_writer()
    store.insert_batch.side_effect = RuntimeError("db is down")

    with pytest.raises(RuntimeError):
        writer._flush([machine_row("h1")])

    records.commit.assert_not_called()


def test_flush_commits_offsets_only_after_the_write():
    writer, records, store = build_writer()
    call_order = []
    store.insert_batch.side_effect = lambda *_: call_order.append("insert")
    records.commit.side_effect = lambda: call_order.append("commit")

    writer._flush([machine_row("h1")])

    assert call_order == ["insert", "commit"]
