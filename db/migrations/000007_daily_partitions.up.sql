-- The partition key cannot be changed in place. Hourly partitions turned a replay
-- with historical timestamps into one part per hour, and ClickHouse ran out of memory.
-- Rows here expire within about 48 hours, so the tables are recreated by day.
-- machines is not partitioned and stays as 000004 created it.
-- 000001 through 000006 stay in the tree: golang-migrate will not run when the
-- file for the version already recorded in schema_migrations is missing.
DROP TABLE IF EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics (
    metric_id    UUID,
    machine_id   LowCardinality(String),
    name         LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    type         LowCardinality(String),
    unit         LowCardinality(String),
    value        Float64,
    attributes   Map(String, String),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (machine_id, name, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;

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
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (machine_id, metric_name, algorithm, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
