import time
from dataclasses import astuple

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.detection import detect, is_scored

WINDOW_MS = 60 * 60 * 1000


class AnomalyDetector(KeyedProcessFunction):
    """Emits an Anomaly row for each anomalous sample and nothing for the rest."""

    def open(self, runtime_context: RuntimeContext):
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.TUPLE([Types.LONG(), Types.DOUBLE()]))
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not is_scored(value):
            return

        event_time = ctx.timestamp()
        window = [(t, v) for t, v in self.recent_values.get() if t >= event_time - WINDOW_MS]
        window.append((event_time, value["value"]))
        self.recent_values.update(window)

        anomaly = detect(value, [v for _, v in window], detected_at=time.time())
        if anomaly is not None:
            # Positional, in dataclass field order, which is the order of ANOMALY_TYPE_INFO.
            yield Row(*astuple(anomaly))
