from pyflink.common.typeinfo import Types

from activity_metrics.anomaly_detector.flink.type_info.row_type_info import RowTypeInfo
from activity_metrics.shared.models.evaluation import Evaluation

EVALUATION_FIELD_TYPES = {
    "metric_id": Types.STRING(),
    "machine_id": Types.STRING(),
    "metric_name": Types.STRING(),
    "timestamp": Types.DOUBLE(),
    "algorithm": Types.STRING(),
    "baseline": Types.DOUBLE(),
    "lower": Types.DOUBLE(),
    "upper": Types.DOUBLE(),
    "is_anomaly": Types.BOOLEAN(),
}

EVALUATION_TYPE_INFO = RowTypeInfo.build(Evaluation, EVALUATION_FIELD_TYPES)
