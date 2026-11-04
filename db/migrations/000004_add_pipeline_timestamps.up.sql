-- pipeline_start is when the collector accepted the metric, pipeline_end when ClickHouse inserted it (was inserted_at).
-- pipeline_end - pipeline_start is how long a metric spends in the pipeline. Rows copied from before have no start.
-- ClickHouse refuses to rename the version column of a ReplacingMergeTree, so metrics is rebuilt and copied.
-- Stop clickhouse-writer first: rows inserted between the copy and the swap are lost.
CREATE TABLE metrics_new (
    metric_id       UUID,
    machine_id      LowCardinality(String),
    name            LowCardinality(String),
    timestamp       DateTime64(3, 'UTC'),
    type            LowCardinality(String),
    unit            LowCardinality(String),
    value           Float64,
    attributes      Map(String, String),
    pipeline_start  Nullable(DateTime64(3, 'UTC')),
    pipeline_end    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(pipeline_end)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (machine_id, name, timestamp, metric_id);

INSERT INTO metrics_new (metric_id, machine_id, name, timestamp, type, unit, value, attributes, pipeline_end)
SELECT metric_id, machine_id, name, timestamp, type, unit, value, attributes, inserted_at FROM metrics;

RENAME TABLE metrics TO metrics_old, metrics_new TO metrics;

DROP TABLE metrics_old;
