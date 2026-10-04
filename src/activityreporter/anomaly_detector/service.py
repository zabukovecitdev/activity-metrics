from dataclasses import astuple

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.detection import evaluate, is_scored

WINDOW_MS = 60 * 60 * 1000


class AnomalyDetector(KeyedProcessFunction):
    """Emits one Evaluation row per detector for each scored sample, anomalous or not."""

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

        for evaluation in evaluate(value, [v for _, v in window]):
            # Positional, in dataclass field order, which is the order of EVALUATION_TYPE_INFO.
            yield Row(*astuple(evaluation))
