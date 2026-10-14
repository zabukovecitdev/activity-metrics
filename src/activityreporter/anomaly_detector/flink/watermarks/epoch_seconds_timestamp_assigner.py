from pyflink.common.watermark_strategy import TimestampAssigner


class EpochSecondsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp"] * 1000)
