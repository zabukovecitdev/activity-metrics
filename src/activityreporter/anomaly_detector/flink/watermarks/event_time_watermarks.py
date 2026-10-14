from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration

from activityreporter.anomaly_detector.flink.watermarks.epoch_seconds_timestamp_assigner import (
    EpochSecondsTimestampAssigner,
)

MAX_OUT_OF_ORDERNESS = Duration.of_seconds(5)


class EventTimeWatermarks:
    @staticmethod
    def build() -> WatermarkStrategy:
        return WatermarkStrategy.for_bounded_out_of_orderness(MAX_OUT_OF_ORDERNESS) \
            .with_timestamp_assigner(EpochSecondsTimestampAssigner())
