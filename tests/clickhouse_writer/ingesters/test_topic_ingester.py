import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic_core import to_jsonable_python

from activityreporter.clickhouse_writer.ingesters.all_ingesters import INGESTERS
from activityreporter.clickhouse_writer.ingesters.evaluations_ingester import EvaluationsIngester
from activityreporter.clickhouse_writer.ingesters.machines_ingester import MachinesIngester
from activityreporter.clickhouse_writer.ingesters.metrics_ingester import MetricsIngester
from activityreporter.clickhouse_writer.ingesters.topic_ingester import TopicIngester
from activityreporter.shared.settings.kafka_settings import KafkaSettings
from tests.support.builders import build_evaluation, build_machine, build_metric, kafka_record, parse_record



INGESTER_CASES = pytest.mark.parametrize(
    "ingester_type, build",
    [(MetricsIngester, build_metric), (EvaluationsIngester, build_evaluation), (MachinesIngester, build_machine)],
    ids=["metrics", "evaluations", "machines"],
)


def test_topic_ingester_is_abstract():
    with pytest.raises(TypeError):
        TopicIngester(MagicMock(), MagicMock())


@INGESTER_CASES
def test_columns_are_the_record_fields(ingester_type, build):
    assert set(ingester_type.columns) == set(to_jsonable_python(build()))


def test_each_ingester_has_its_own_group():
    assert sorted(w.group_id() for w in INGESTERS) == [
        "clickhouse-evaluations-writer", "clickhouse-machines-writer", "clickhouse-metrics-writer",
    ]


def test_topics_follow_the_producers_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_EVALUATIONS_TOPIC", "scores")

    kafka = KafkaSettings()

    assert EvaluationsIngester.topic(kafka) == "scores"
    assert MetricsIngester.topic(kafka) == "raw_metrics"
    assert MachinesIngester.topic(kafka) == "machines"


@INGESTER_CASES
def test_parse_ignores_unknown_fields(ingester_type, build):
    assert parse_record(ingester_type, {**to_jsonable_python(build()), "added_later": "x"}) == parse_record(ingester_type, to_jsonable_python(build()))


@pytest.mark.parametrize(
    "ingester_type, build", [(MetricsIngester, build_metric), (EvaluationsIngester, build_evaluation)],
    ids=["metrics", "evaluations"],
)
@pytest.mark.parametrize("metric_id", [None, "not-a-uuid", 5])
def test_parse_skips_records_without_a_valid_metric_id(ingester_type, build, metric_id):
    assert parse_record(ingester_type, {**to_jsonable_python(build()), "metric_id": metric_id}) is None


@INGESTER_CASES
@pytest.mark.parametrize("value", [b"not json", b"[]", json.dumps({"name": "x"}).encode()])
def test_parse_skips_malformed_records(ingester_type, build, value):
    assert parse_record(ingester_type, value) is None


class LoopBreak(Exception):
    pass


def machine_record(machine_id: str) -> MagicMock:
    return kafka_record({**to_jsonable_python(build_machine()), "machine_id": machine_id})


def machine_row(machine_id: str) -> list:
    return MachinesIngester(MagicMock(), MagicMock())._parse(machine_record(machine_id))


def build_ingester(batch_size=100, batch_timeout_seconds=5.0):
    records, store = MagicMock(), MagicMock()
    return MachinesIngester(records, store, batch_size, batch_timeout_seconds), records, store


def inserted(store) -> list:
    """Rows passed to insert_batch, in call order."""
    return [row for call in store.insert_batch.call_args_list for row in call.args[2]]


def test_run_flushes_when_the_batch_is_full():
    ingester, records, store = build_ingester(batch_size=2)
    records.poll.side_effect = [[machine_record("h1")], [machine_record("h2")], LoopBreak]

    with pytest.raises(LoopBreak):
        ingester.run()

    assert inserted(store) == [machine_row("h1"), machine_row("h2")]
    assert store.insert_batch.call_args.args[:2] == ("machines", MachinesIngester.columns)
    assert records.commit.call_count == 1


def test_run_flushes_on_timeout_before_batch_is_full():
    ingester, records, store = build_ingester(batch_size=100, batch_timeout_seconds=5.0)
    records.poll.side_effect = [[machine_record("h1")], LoopBreak]

    with patch("activityreporter.clickhouse_writer.ingesters.topic_ingester.time.monotonic", side_effect=[0.0, 10.0, 10.0]), \
         pytest.raises(LoopBreak):
        ingester.run()

    assert inserted(store) == [machine_row("h1")]
    assert records.commit.call_count == 1


def test_run_does_not_flush_empty_batch():
    ingester, records, store = build_ingester(batch_size=1)
    records.poll.side_effect = [[], LoopBreak]

    with pytest.raises(LoopBreak):
        ingester.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_run_does_not_flush_when_every_record_is_malformed():
    ingester, records, store = build_ingester(batch_size=1)
    records.poll.side_effect = [[kafka_record(b"not json")], LoopBreak]

    with pytest.raises(LoopBreak):
        ingester.run()

    store.insert_batch.assert_not_called()
    records.commit.assert_not_called()


def test_stop_flushes_pending_batch():
    ingester, records, store = build_ingester(batch_size=100, batch_timeout_seconds=1000.0)

    def poll_then_stop(**_):
        ingester.stop()
        return [machine_record("h1")]

    records.poll.side_effect = poll_then_stop

    ingester.run()

    assert inserted(store) == [machine_row("h1")]
    assert records.commit.call_count == 1


def test_flush_does_not_commit_offsets_when_the_write_fails():
    ingester, records, store = build_ingester()
    store.insert_batch.side_effect = RuntimeError("db is down")

    with pytest.raises(RuntimeError):
        ingester._flush([machine_row("h1")])

    records.commit.assert_not_called()


def test_flush_commits_offsets_only_after_the_write():
    ingester, records, store = build_ingester()
    call_order = []
    store.insert_batch.side_effect = lambda *_: call_order.append("insert")
    records.commit.side_effect = lambda: call_order.append("commit")

    ingester._flush([machine_row("h1")])

    assert call_order == ["insert", "commit"]
