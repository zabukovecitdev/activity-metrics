import json
from unittest.mock import MagicMock

from pydantic_core import to_jsonable_python

from activityreporter.shared.models.evaluation import Evaluation
from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metric import Metric

METRIC_ID = "01a0e974-dc94-74df-ae87-e0e6064b18ef"


def build_metric(timestamp: float = 1.0) -> Metric:
    return Metric(
        timestamp=timestamp,
        name="system.cpu.utilization",
        type="gauge",
        unit="%",
        value=15.0,
        machine_id="machine-123",
        metric_id=METRIC_ID,
        attributes={"core": "0"},
    )


def build_evaluation(timestamp: float = 1.0) -> Evaluation:
    return Evaluation(
        metric_id=METRIC_ID,
        machine_id="machine-123",
        metric_name="system.cpu.utilization",
        timestamp=timestamp,
        algorithm="ewma",
        baseline=21.3,
        lower=None,
        upper=31.3,
        is_anomaly=True,
    )


def build_machine(timestamp: float = 1.0) -> Machine:
    return Machine(
        machine_id="machine-123",
        hostname="workstation",
        os="linux",
        os_version="6.18.0",
        architecture="x86_64",
        cores=16,
        total_memory=34_359_738_368,
        total_disk=1_000_204_886_016,
        last_boot="2026-10-01T06:30:00+00:00",
        observed_at=timestamp,
    )


def kafka_record(payload) -> MagicMock:
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return MagicMock(value=value)


def parse_record(sink_type, payload):
    return sink_type(MagicMock(), MagicMock())._parse(kafka_record(payload))


def row_of(sink_type, record) -> dict:
    return dict(zip(sink_type.columns, parse_record(sink_type, to_jsonable_python(record)), strict=True))
