from datetime import datetime, timezone

import pytest
from pydantic_core import to_jsonable_python

from activityreporter.clickhouse_writer.ingesters.machines_ingester import MachinesIngester
from tests.support.builders import build_machine, parse_record, row_of


def test_machine_row_converts_both_times_to_utc():
    row = row_of(MachinesIngester, build_machine(1.5))

    assert row["last_boot"] == datetime(2026, 10, 1, 6, 30, tzinfo=timezone.utc)
    assert row["observed_at"] == datetime.fromtimestamp(1.5, timezone.utc)
    assert (row["cores"], row["total_memory"]) == (16, 34_359_738_368)


@pytest.mark.parametrize(
    "overrides",
    [{"machine_id": ""}, {"last_boot": "yesterday"}, {"last_boot": "2026-10-01T06:30:00"}],
    ids=["empty-id", "not-iso", "no-offset"],
)
def test_parse_skips_invalid_machines(overrides):
    assert parse_record(MachinesIngester, {**to_jsonable_python(build_machine()), **overrides}) is None
