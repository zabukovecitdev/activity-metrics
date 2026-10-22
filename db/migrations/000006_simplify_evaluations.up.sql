-- Keeps only what the dashboard draws: each algorithm's line, band and anomaly flag.
-- metric_id is the sample's id in metrics, which keeps its value.
-- Rows expire after 24 hours anyway, so the old table is dropped, not converted.
DROP TABLE IF EXISTS evaluations;

CREATE TABLE IF NOT EXISTS evaluations (
    metric_id    UUID,
    machine_id   LowCardinality(String),
    metric_name  LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    algorithm    LowCardinality(String),
    baseline     Float64,
    lower        Nullable(Float64),
    upper        Float64,
    is_anomaly   Bool
)
-- A replayed row has the same key and collapses on merge.
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine_id, metric_name, algorithm, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
