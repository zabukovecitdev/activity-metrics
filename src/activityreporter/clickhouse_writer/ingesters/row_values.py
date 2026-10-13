import uuid
from datetime import datetime, timezone
from typing import Any


class RowValues:
    @staticmethod
    def utc(epoch_seconds: float) -> datetime:
        return datetime.fromtimestamp(epoch_seconds, timezone.utc)

    @staticmethod
    def metric_uuid(metric_id: Any) -> uuid.UUID:
        # str() turns None or a number into an invalid hex string, so every bad id raises ValueError.
        return uuid.UUID(str(metric_id))
