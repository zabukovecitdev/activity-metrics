from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.services.metric_evaluator import MetricEvaluator

# A count, not a time span, so the window holds enough values for every detector at any sample rate.
# One hour at the 10-second scrape interval.
WINDOW_POINTS = 360


class AnomalyDetectionFunction(KeyedProcessFunction):
    """Emits one Evaluation row per detector for each scored sample, anomalous or not."""

    def open(self, runtime_context: RuntimeContext):
        self.metrics_group = runtime_context.get_metrics_group()
        self.evaluations_emitted = self.metrics_group.counter("evaluations_emitted")
        # Per detector, as a count and a total of microseconds: the mean is rate(score_micros) / rate(scores).
        self.score_metrics: dict[str, tuple] = {}
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.DOUBLE())
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not MetricEvaluator.is_scored(value):
            return

        window = [*self.recent_values.get(), value["value"]][-WINDOW_POINTS:]
        self.recent_values.update(window)

        for evaluation in MetricEvaluator.evaluate(value, window, observe=self._observe):
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
