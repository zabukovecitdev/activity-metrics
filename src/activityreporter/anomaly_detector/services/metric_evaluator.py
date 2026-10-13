from collections.abc import Mapping
from typing import Any

from activityreporter.anomaly_detector.detectors.detector import Detector
from activityreporter.anomaly_detector.detectors.detector_registry import DetectorRegistry
from activityreporter.shared.models.evaluation import Evaluation


class MetricEvaluator:
    # Flags, near-constant gauges and ever-growing counters would be flagged on every small step.
    NOT_ANOMALY_SCORED = {
        "system.battery.charging",
        "system.battery.utilization",
        "system.memory.limit",
        "system.uptime",
    }

    @classmethod
    def is_scored(cls, metric: Mapping[str, Any]) -> bool:
        return metric["type"] == "gauge" and metric["name"] not in cls.NOT_ANOMALY_SCORED

    @staticmethod
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
        for detector in detectors if detectors is not None else DetectorRegistry.for_metric(metric["name"]):
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
