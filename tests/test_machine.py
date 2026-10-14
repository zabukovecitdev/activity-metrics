import json

import pytest
from pydantic_core import to_jsonable_python

from activityreporter.shared.models.machine import Machine
from tests.support.builders import build_machine


@pytest.mark.parametrize(
    "overrides", [{"machine_id": ""}, {"cores": -1}, {"last_boot": "2026-10-01T06:30:00"}, {"hostname": None}],
)
def test_malformed_machine_raises_a_value_error(overrides):
    with pytest.raises(ValueError):
        Machine.model_validate_json(json.dumps({**to_jsonable_python(build_machine()), **overrides}))


def test_a_body_that_is_not_json_raises_a_value_error():
    with pytest.raises(ValueError):
        Machine.model_validate_json("<html>502 Bad Gateway</html>")
