from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.services.metric_evaluator import MetricEvaluator

WINDOW_MS = 60 * 60 * 1000


class AnomalyDetectionFunction(KeyedProcessFunction):
    """Emits one Evaluation row per detector for each scored sample, anomalous or not."""

    def open(self, runtime_context: RuntimeContext):
        self.metrics_group = runtime_context.get_metrics_group()
        self.evaluations_emitted = self.metrics_group.counter("evaluations_emitted")
        # Per detector, as a count and a total of microseconds: the mean is rate(score_micros) / rate(scores).
        self.score_metrics: dict[str, tuple] = {}
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.TUPLE([Types.LONG(), Types.DOUBLE()]))
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not MetricEvaluator.is_scored(value):
            return

        event_time = ctx.timestamp()
        window = [(t, v) for t, v in self.recent_values.get() if t >= event_time - WINDOW_MS]
        window.append((event_time, value["value"]))
        self.recent_values.update(window)

        for evaluation in MetricEvaluator.evaluate(value, [v for _, v in window], observe=self._observe):
            self.evaluations_emitted.inc()
            # Positional, in model field order, which is the order of EVALUATION_TYPE_INFO.
            yield Row(*evaluation.model_dump().values())

    def _observe(self, algorithm: str, seconds: float) -> None:
        if algorithm not in self.score_metrics:
            group = self.metrics_group.add_group("algorithm", algorithm)
            self.score_metrics[algorithm] = (group.counter("scores"), group.counter("score_micros"))
        scores, micros = self.score_metrics[algorithm]
        scores.inc()
        micros.inc(round(seconds * 1_000_000))
