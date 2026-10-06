from collections.abc import Mapping
from typing import Any

from activityreporter.anomaly_detector.detectors import (
    Detector,
    EwmaDetector,
    MadDetector,
)
from activityreporter.shared.models import Evaluation

# Detectors per metric. The floors are the smallest deviation worth flagging for that metric:
# a series that barely moves would otherwise be flagged on every small step.
METRIC_DETECTORS: dict[str, list[Detector]] = {
    # Percentage points.
    "system.cpu.utilization": [MadDetector(min_deviation=10.0), EwmaDetector(min_deviation=10.0)],
    # Memory in use sits near one level and drifts, so only a change of 2% of it counts.
    "system.memory.usage": [
        MadDetector(min_relative=0.02),
        EwmaDetector(min_deviation=0.0, min_relative=0.02),
    ],
}
# For any other scored metric, whose units aren't known: 1% of its level.
DEFAULT_DETECTORS: list[Detector] = [
    MadDetector(min_relative=0.01),
    EwmaDetector(min_deviation=0.0, min_relative=0.01),
]


def detectors_for(metric_name: str) -> list[Detector]:
    return METRIC_DETECTORS.get(metric_name, DEFAULT_DETECTORS)
# Flags, near-constant gauges and ever-growing counters would be flagged on every small step.
NOT_ANOMALY_SCORED = {
    "system.battery.charging",
    "system.battery.utilization",
    "system.memory.limit",
    "system.uptime",
}


def is_scored(metric: Mapping[str, Any]) -> bool:
    return metric["type"] == "gauge" and metric["name"] not in NOT_ANOMALY_SCORED


def evaluate(
    metric: Mapping[str, Any],
    window: list[float],
    detectors: list[Detector] | None = None,
) -> list[Evaluation]:
    """One evaluation per detector for `metric`, the last value of `window`.

    A detector without enough values yet is skipped, so a series has no
    evaluations from it while it warms up. Kept free of PyFlink so it can be
    tested without a Flink runtime.
    """
    evaluations: list[Evaluation] = []
    for detector in detectors if detectors is not None else detectors_for(metric["name"]):
        if len(window) < detector.min_points:
            continue
        result = detector.evaluate(window)
        evaluations.append(Evaluation(
            metric_id=metric["metric_id"],
            machine_id=metric["machine_id"],
            metric_name=metric["name"],
            timestamp=metric["timestamp"],
            algorithm=detector.name,
            baseline=result.baseline,
            lower=result.lower,
            upper=result.upper,
            is_anomaly=result.is_anomaly,
        ))
    return evaluations
