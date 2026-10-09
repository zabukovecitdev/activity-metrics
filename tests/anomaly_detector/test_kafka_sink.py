import ast
from pathlib import Path

REPOSITORY = (
    Path(__file__).resolve().parents[2]
    / "src/activityreporter/anomaly_detector/repository.py"
)


def test_evaluations_sink_flushes_to_kafka_before_a_checkpoint():
    """The sink must ack before a checkpoint, or a restore drops evaluations.

    PyFlink is not importable on the runtime that runs these tests, so this
    checks the builder call in source. kafka_evaluations_sink is only built
    inside the Flink job.
    """
    function = _function(_parse(), "kafka_evaluations_sink")
    calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_delivery_guarantee"
    ]

    assert len(calls) == 1
    guarantee = calls[0].args[0]
    assert isinstance(guarantee, ast.Attribute)
    assert guarantee.attr == "AT_LEAST_ONCE"
    assert isinstance(guarantee.value, ast.Name)
    assert guarantee.value.id == "DeliveryGuarantee"


def _parse() -> ast.Module:
    return ast.parse(REPOSITORY.read_text())


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} is not defined in {REPOSITORY.name}")
