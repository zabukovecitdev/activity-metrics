"""The Flink job's source, sink, and row types, without a PyFlink runtime.

PyFlink is not installed in the test environment (the job runs on Python 3.10/3.11).
These stubs record the builder calls `main` makes so a missing module or a drifted
consumer group, offset, delivery guarantee, or field order fails here.
"""

import sys
import types

import pytest
from pydantic import BaseModel

JOB_MODULES = (
    "activityreporter.anomaly_detector.main",
    "activityreporter.anomaly_detector.flink",
)


class _Builder:
    def __init__(self):
        self.calls = {}

    def __getattr__(self, name):
        def method(*args, **kwargs):
            self.calls[name] = args
            return self

        return method


class _Duration:
    def __init__(self, seconds):
        self.seconds = seconds


class _NamedRow:
    def __init__(self, names, types_):
        self.names = list(names)
        self.types = list(types_)


def _ensure(name):
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
        if "." in name:
            parent, child = name.rsplit(".", 1)
            _ensure(parent)
            setattr(sys.modules[parent], child, sys.modules[name])
    return sys.modules[name]


def _install_pyflink():
    common = _ensure("pyflink.common")
    typeinfo = _ensure("pyflink.common.typeinfo")
    time = _ensure("pyflink.common.time")
    watermark_strategy = _ensure("pyflink.common.watermark_strategy")
    datastream = _ensure("pyflink.datastream")
    functions = _ensure("pyflink.datastream.functions")
    state = _ensure("pyflink.datastream.state")
    connectors_base = _ensure("pyflink.datastream.connectors.base")
    kafka = _ensure("pyflink.datastream.connectors.kafka")
    formats = _ensure("pyflink.datastream.formats.json")

    class TimestampAssigner:
        pass

    class KeyedProcessFunction:
        class Context:
            pass

    class Types:
        @staticmethod
        def DOUBLE():
            return "DOUBLE"

        @staticmethod
        def STRING():
            return "STRING"

        @staticmethod
        def BOOLEAN():
            return "BOOLEAN"

        @staticmethod
        def LONG():
            return "LONG"

        @staticmethod
        def MAP(key, value):
            return ("MAP", key, value)

        @staticmethod
        def TUPLE(type_list):
            return ("TUPLE", type_list)

        @staticmethod
        def ROW_NAMED(names, type_list):
            return _NamedRow(names, type_list)

    class Duration:
        @staticmethod
        def of_seconds(seconds):
            return _Duration(seconds)

    class WatermarkStrategy:
        @staticmethod
        def for_bounded_out_of_orderness(bound):
            builder = _Builder()
            builder.bound = bound
            return builder

    class KafkaOffsetsInitializer:
        @staticmethod
        def earliest():
            return "earliest"

    class KafkaSource:
        @staticmethod
        def builder():
            return _Builder()

    class KafkaSink:
        @staticmethod
        def builder():
            return _Builder()

    class KafkaRecordSerializationSchema:
        @staticmethod
        def builder():
            return _Builder()

    class JsonRowDeserializationSchema:
        @staticmethod
        def builder():
            return _Builder()

    class JsonRowSerializationSchema:
        @staticmethod
        def builder():
            return _Builder()

    class DeliveryGuarantee:
        AT_LEAST_ONCE = "AT_LEAST_ONCE"

    common.Configuration = type("Configuration", (), {})
    common.Row = type("Row", (), {"__init__": lambda self, *values: None})
    common.WatermarkStrategy = WatermarkStrategy
    typeinfo.Types = Types
    time.Duration = Duration
    watermark_strategy.TimestampAssigner = TimestampAssigner
    datastream.CheckpointingMode = type("CheckpointingMode", (), {"EXACTLY_ONCE": "EXACTLY_ONCE"})
    datastream.StreamExecutionEnvironment = type("StreamExecutionEnvironment", (), {})
    functions.KeyedProcessFunction = KeyedProcessFunction
    functions.RuntimeContext = type("RuntimeContext", (), {})
    state.ListStateDescriptor = type("ListStateDescriptor", (), {})
    connectors_base.DeliveryGuarantee = DeliveryGuarantee
    kafka.KafkaOffsetsInitializer = KafkaOffsetsInitializer
    kafka.KafkaSource = KafkaSource
    kafka.KafkaSink = KafkaSink
    kafka.KafkaRecordSerializationSchema = KafkaRecordSerializationSchema
    formats.JsonRowDeserializationSchema = JsonRowDeserializationSchema
    formats.JsonRowSerializationSchema = JsonRowSerializationSchema


@pytest.fixture
def pyflink_stub():
    _install_pyflink()
    yield
    for name in list(sys.modules):
        if name.startswith("pyflink") or name.startswith(JOB_MODULES):
            del sys.modules[name]


def test_job_entrypoint_imports(pyflink_stub):
    import activityreporter.anomaly_detector.main  # noqa: F401


def test_metric_source_reads_raw_metrics_from_the_start(pyflink_stub):
    from activityreporter.anomaly_detector.flink.sources.kafka_metrics_source import KafkaMetricsSource
    from activityreporter.anomaly_detector.flink.type_info.metric_type_info import METRIC_TYPE_INFO
    from activityreporter.shared.models.metric import Metric
    from activityreporter.shared.settings.kafka_settings import KafkaSettings

    kafka = KafkaSettings(connection_string="kafka:9092", raw_metrics_topic="raw_metrics_test")
    source = KafkaMetricsSource.build(kafka)
    deserializer = source.calls["set_value_only_deserializer"][0]

    assert source.calls["set_bootstrap_servers"] == ("kafka:9092",)
    assert source.calls["set_topics"] == ("raw_metrics_test",)
    assert source.calls["set_group_id"] == ("anomaly-detector",)
    assert source.calls["set_starting_offsets"] == ("earliest",)
    assert deserializer.calls["type_info"] == (METRIC_TYPE_INFO,)
    assert METRIC_TYPE_INFO.names == list(Metric.model_fields)
    assert METRIC_TYPE_INFO.types[METRIC_TYPE_INFO.names.index("attributes")] == ("MAP", "STRING", "STRING")


def test_evaluations_sink_is_at_least_once_in_model_field_order(pyflink_stub):
    from activityreporter.anomaly_detector.flink.sinks.kafka_evaluations_sink import KafkaEvaluationsSink
    from activityreporter.anomaly_detector.flink.type_info.evaluation_type_info import EVALUATION_TYPE_INFO
    from activityreporter.shared.models.evaluation import Evaluation
    from activityreporter.shared.settings.kafka_settings import KafkaSettings

    kafka = KafkaSettings(connection_string="kafka:9092", evaluations_topic="evaluations_test")
    sink = KafkaEvaluationsSink.build(kafka)
    serializer = sink.calls["set_record_serializer"][0]
    schema = serializer.calls["set_value_serialization_schema"][0]

    assert sink.calls["set_bootstrap_servers"] == ("kafka:9092",)
    assert sink.calls["set_delivery_guarantee"] == ("AT_LEAST_ONCE",)
    assert serializer.calls["set_topic"] == ("evaluations_test",)
    assert schema.calls["with_type_info"] == (EVALUATION_TYPE_INFO,)
    assert EVALUATION_TYPE_INFO.names == list(Evaluation.model_fields)


def test_event_time_is_the_sample_timestamp_with_five_seconds_of_lateness(pyflink_stub):
    from activityreporter.anomaly_detector.flink.watermarks.epoch_seconds_timestamp_assigner import (
        EpochSecondsTimestampAssigner,
    )
    from activityreporter.anomaly_detector.flink.watermarks.event_time_watermarks import EventTimeWatermarks

    watermarks = EventTimeWatermarks.build()

    assert watermarks.bound.seconds == 5
    assert isinstance(watermarks.calls["with_timestamp_assigner"][0], EpochSecondsTimestampAssigner)
    assert EpochSecondsTimestampAssigner().extract_timestamp({"timestamp": 1759054560.5}, 0) == 1759054560500


def test_row_type_info_rejects_fields_that_drift_from_the_model(pyflink_stub):
    from activityreporter.anomaly_detector.flink.type_info.row_type_info import RowTypeInfo
    from pyflink.common.typeinfo import Types

    class Sample(BaseModel):
        value: float

    with pytest.raises(RuntimeError, match="out of sync"):
        RowTypeInfo.build(Sample, {"value": Types.DOUBLE(), "extra": Types.STRING()})
