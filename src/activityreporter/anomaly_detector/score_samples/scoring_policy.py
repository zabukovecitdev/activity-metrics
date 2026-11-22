from collections.abc import Mapping
from typing import Any

from activityreporter.anomaly_detector.detectors.detector import Detector
from activityreporter.anomaly_detector.detectors.ewma import EWMA
from activityreporter.anomaly_detector.detectors.mad import MAD


class ScoringPolicy:
    # The only scored metrics, with their detectors. The floors are the smallest deviation worth flagging for that
    # metric: a series that barely moves would otherwise be flagged on every small step.
    DETECTORS: dict[str, list[Detector]] = {
        # Percentage points.
        "system.cpu.utilization": [MAD(min_deviation=10.0), EWMA(min_deviation=10.0)],
        # Memory in use sits near one level and drifts, so only a change of 2% of it counts.
        "system.memory.usage": [MAD(min_relative=0.02), EWMA(min_deviation=0.0, min_relative=0.02)],
    }
    # A replayed NAB series is named NAB_PREFIX + its file name. NAB values range from 0.1 to millions,
    # so no floor in the values' units fits them all, and these detectors have none.
    NAB_PREFIX = "nab."
    NAB_DETECTORS: list[Detector] = [MAD(), EWMA(min_deviation=0.0)]

    @classmethod
    def detectors(cls, name: str) -> list[Detector]:
        """The detectors that score the metric `name`, none for an unscored metric."""
        if name.startswith(cls.NAB_PREFIX):
            return cls.NAB_DETECTORS
        return cls.DETECTORS.get(name, [])

    @classmethod
    def is_scored(cls, metric: Mapping[str, Any]) -> bool:
        """Whether `metric` has any detector, so whether the pipeline scores it at all."""
        return bool(cls.detectors(metric["name"]))
