import time
from collections.abc import Callable, Mapping
from typing import Any

from activityreporter.anomaly_detector.score_samples.scoring_policy import ScoringPolicy
from activityreporter.contracts.evaluation import Evaluation


class MetricEvaluator:
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
        for detector in ScoringPolicy.detectors(metric["name"]):
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
