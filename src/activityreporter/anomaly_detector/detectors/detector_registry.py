from activityreporter.anomaly_detector.detectors.detector import Detector
from activityreporter.anomaly_detector.detectors.ewma_detector import EwmaDetector
from activityreporter.anomaly_detector.detectors.mad_detector import MadDetector


class DetectorRegistry:
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

    @classmethod
    def for_metric(cls, metric_name: str) -> list[Detector]:
        return cls.METRIC_DETECTORS.get(metric_name, cls.DEFAULT_DETECTORS)
