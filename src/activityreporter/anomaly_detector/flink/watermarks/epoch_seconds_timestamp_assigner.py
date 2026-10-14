from pyflink.common.watermark_strategy import TimestampAssigner


class EpochSecondsTimestampAssigner(TimestampAssigner):
    """Event time is the metric's `timestamp` in epoch seconds. Kafka record timestamps are not used."""

    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp"] * 1000)
