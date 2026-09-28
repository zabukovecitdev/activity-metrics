from collections.abc import Mapping
from typing import Any

from activityreporter.anomaly_detector.mad import MAD
from activityreporter.shared.anomalies import Anomaly

MIN_VALUES_FOR_MAD = 20
# MAD's deviation is 0 for flags and near-constant gauges, so every small step would be flagged.
NOT_ANOMALY_SCORED = {"system.battery.charging", "system.battery.utilization", "system.memory.limit"}


def is_scored(metric: Mapping[str, Any]) -> bool:
    return metric["type"] == "gauge" and metric["name"] not in NOT_ANOMALY_SCORED


def detect(metric: Mapping[str, Any], window: list[float], detected_at: float) -> Anomaly | None:
    """The anomaly for `metric`, the last value of `window`, or None when it isn't one.

    Kept free of PyFlink so it can be tested without a Flink runtime.
    """
    if len(window) < MIN_VALUES_FOR_MAD:
        return None

    score = MAD().score(window)
    if score.score < MAD.THRESHOLD:
        return None

    return Anomaly(
        metric_id=metric["metric_id"],
        machine_id=metric["machine_id"],
        metric_name=metric["name"],
        metric_attributes=metric["attributes"],
        timestamp=metric["timestamp"],
        value=metric["value"],
        algorithm="mad",
        score=score.score,
        threshold=MAD.THRESHOLD,
        direction="up" if metric["value"] > score.median else "down",
        details={"median": score.median, "scale": score.scale, "window_size": float(len(window))},
        detected_at=detected_at,
    )
