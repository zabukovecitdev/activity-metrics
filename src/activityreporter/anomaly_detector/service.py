from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.mad import MAD

WINDOW_MS = 60 * 60 * 1000
MIN_VALUES_FOR_MAD = 20
# MAD's deviation is 0 for flags and near-constant gauges, so every small step would be flagged.
NOT_ANOMALY_SCORED = {"system.battery.charging", "system.battery.utilization", "system.memory.limit"}


class AnomalyDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.TUPLE([Types.LONG(), Types.DOUBLE()]))
        )
        self.mad = MAD()

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if value["type"] != "gauge" or value["name"] in NOT_ANOMALY_SCORED:
            value["is_anomaly"] = False
            yield value
            return

        event_time = ctx.timestamp()
        window = [(t, v) for t, v in self.recent_values.get() if t >= event_time - WINDOW_MS]
        window.append((event_time, value["value"]))
        self.recent_values.update(window)

        values = [v for _, v in window]
        value["is_anomaly"] = len(values) >= MIN_VALUES_FOR_MAD and self.mad.is_anomaly(values)
        yield value
