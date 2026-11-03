from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from activityreporter.anomaly_detector.algorithms.detector import WINDOW_POINTS
from activityreporter.anomaly_detector.services.metric_evaluator import MetricEvaluator


class AnomalyDetectionFunction(KeyedProcessFunction):
    """Emits one Evaluation row per detector for each scored sample, anomalous or not."""

    def open(self, runtime_context: RuntimeContext):
        """Creates the counters and the state that keep the recent values.

        Args:
            runtime_context: The Flink runtime context of this task.
        """
        self.metrics_group = runtime_context.get_metrics_group()
        self.evaluations_emitted = self.metrics_group.counter("evaluations_emitted")
        # Per detector, as a count and a total of microseconds: the mean is rate(score_micros) / rate(scores).
        self.score_metrics: dict[str, tuple] = {}
        self.recent_values = runtime_context.get_list_state(
            ListStateDescriptor("recent_values", Types.DOUBLE())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        """Scores one metric sample and yields one evaluation row for each detector.

        Args:
            value: The metric row. It has the `machine_id`, `name` and `value` fields.
            ctx: The Flink process context.

        Yields:
            One row for each evaluation, in the field order of `EVALUATION_TYPE_INFO`.
        """
        if not MetricEvaluator.is_scored(value):
            return

        window = [*self.recent_values.get(), value["value"]][-WINDOW_POINTS:]
        self.recent_values.update(window)

        for evaluation in MetricEvaluator.evaluate(
            value, window, observe=self._observe
        ):
            self.evaluations_emitted.inc()
            # Positional, in model field order, which is the order of EVALUATION_TYPE_INFO.
            yield Row(*evaluation.model_dump().values())

    def _observe(self, algorithm: str, seconds: float) -> None:
        """Records one score of a detector and the time it took.

        Args:
            algorithm: The name of the detector.
            seconds: The time the score took, in seconds.
        """
        if algorithm not in self.score_metrics:
            group = self.metrics_group.add_group("algorithm", algorithm)
            self.score_metrics[algorithm] = (
                group.counter("scores"),
                group.counter("score_micros"),
            )
        scores, micros = self.score_metrics[algorithm]
        scores.inc()
        micros.inc(round(seconds * 1_000_000))
