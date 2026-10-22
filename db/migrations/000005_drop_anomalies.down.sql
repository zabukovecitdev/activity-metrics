-- Recreates the anomalies table from 000002, which evaluations replaced.
-- (machine_id, metric_name, timestamp, metric_id) is the sample's key in
-- metrics, so the two join on metrics' sorting key. Keyed like metrics plus
-- algorithm, so a replayed batch collapses on merge.
CREATE TABLE IF NOT EXISTS anomalies (
    schema_version     UInt16,
    metric_id          UUID,
    machine_id         LowCardinality(String),
    metric_name        LowCardinality(String),
    metric_attributes  Map(String, String),
    timestamp          DateTime64(3, 'UTC'),
    value              Float64,
    algorithm          LowCardinality(String),
    score              Float64,
    threshold          Float64,
    direction          LowCardinality(String),
    details            Map(LowCardinality(String), Float64),
    detected_at        DateTime64(3, 'UTC'),
    ingested_at        DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine_id, metric_name, timestamp, metric_id, algorithm)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
