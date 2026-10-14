import time
from collections.abc import Callable, Mapping
from typing import Any

from activityreporter.anomaly_detector.algorithms.detector import Detector
from activityreporter.anomaly_detector.algorithms.ewma import EWMA
from activityreporter.anomaly_detector.algorithms.mad import MAD
from activityreporter.shared.models.evaluation import Evaluation


class MetricEvaluator:
    # The only scored metrics, with their detectors. The floors are the smallest deviation worth flagging for that
    # metric: a series that barely moves would otherwise be flagged on every small step.
    DETECTORS: dict[str, list[Detector]] = {
        # Percentage points.
        "system.cpu.utilization": [MAD(min_deviation=10.0), EWMA(min_deviation=10.0)],
        # Memory in use sits near one level and drifts, so only a change of 2% of it counts.
        "system.memory.usage": [MAD(min_relative=0.02), EWMA(min_deviation=0.0, min_relative=0.02)],
    }

    @classmethod
    def is_scored(cls, metric: Mapping[str, Any]) -> bool:
        return metric["name"] in cls.DETECTORS

    @classmethod
    def evaluate(
        cls,
        metric: Mapping[str, Any],
        window: list[float],
        observe: Callable[[str, float], None] | None = None,
    ) -> list[Evaluation]:
        """One evaluation per detector for `metric`, a scored metric and the last value of `window`.

        A detector without enough values yet is skipped, so a series has no
        evaluations from it while it warms up. Kept free of PyFlink so it can be
        tested without a Flink runtime. `observe` gets each detector's name and the seconds it took.
        """
        evaluations: list[Evaluation] = []
        for detector in cls.DETECTORS[metric["name"]]:
            if len(window) < detector.min_points:
                continue
            started = time.perf_counter()
            result = detector.evaluate(window)
            if observe:
                observe(detector.name, time.perf_counter() - started)
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
