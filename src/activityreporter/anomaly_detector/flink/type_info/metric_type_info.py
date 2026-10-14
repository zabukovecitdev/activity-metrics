from pyflink.common.typeinfo import Types

from activityreporter.anomaly_detector.flink.type_info.row_type_info import RowTypeInfo
from activityreporter.shared.models.metric import Metric

# Order is not significant here: RowTypeInfo puts the row in Metric's field order.
METRIC_FIELD_TYPES = {
    "timestamp": Types.DOUBLE(),
    "name": Types.STRING(),
    "type": Types.STRING(),
    "unit": Types.STRING(),
    "value": Types.DOUBLE(),
    "machine_id": Types.STRING(),
    "metric_id": Types.STRING(),
    "attributes": Types.MAP(Types.STRING(), Types.STRING()),
}

METRIC_TYPE_INFO = RowTypeInfo.build(Metric, METRIC_FIELD_TYPES)
