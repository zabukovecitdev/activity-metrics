import logging

from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(level=logging.INFO)

# `flink-metrics-aggregator` per flink-jobs/README.md — must stay distinct from
# `timescale-writer` so both consumers receive every message on the topic.
KAFKA_GROUP_ID = "flink-metrics-aggregator"

SOURCE_DDL = f"""
    CREATE TABLE metrics_source (
        machine_id    STRING,
        `timestamp`   DOUBLE,
        cpu_usage     DOUBLE,
        memory_usage  DOUBLE,
        memory_total  DOUBLE,
        labels        MAP<STRING, STRING>,
        event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'metrics',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = '{KAFKA_GROUP_ID}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
"""

# Placeholder sink: prints every row the job reads from Kafka to the
# taskmanager's stdout, so the wiring can be verified end to end before any
# real aggregation logic is written.
SINK_DDL = """
    CREATE TABLE metrics_sink (
        machine_id    STRING,
        event_time    TIMESTAMP_LTZ(3),
        cpu_usage     DOUBLE,
        memory_usage  DOUBLE
    ) WITH (
        'connector' = 'print'
    )
"""


INSERT_SQL = """
    INSERT INTO metrics_sink
    SELECT machine_id, event_time, cpu_usage, memory_usage
    FROM metrics_source
"""


def main() -> None:
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    env.execute_sql(SOURCE_DDL)
    env.execute_sql(SINK_DDL)
    env.execute_sql(INSERT_SQL)


if __name__ == "__main__":
    main()
