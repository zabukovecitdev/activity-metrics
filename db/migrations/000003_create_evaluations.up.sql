-- One row per scored metric sample per algorithm, matching shared.evaluations.Evaluation.
-- metric_id is the sample's id in metrics. machine_id, metric_name and timestamp
-- are copied so the table is sorted like metrics and reads on its own.
-- Fixed columns (baseline, lower, upper, score, threshold, is_anomaly, direction)
-- are what every algorithm provides. params holds its inputs and details what it
-- computed, so a new algorithm needs no schema change.
CREATE TABLE IF NOT EXISTS evaluations (
    schema_version     UInt16,
    metric_id          UUID,
    machine_id         LowCardinality(String),
    metric_name        LowCardinality(String),
    timestamp          DateTime64(3, 'UTC'),
    value              Float64,
    algorithm          LowCardinality(String),
    algorithm_version  UInt16,
    params             Map(LowCardinality(String), Float64),
    baseline           Nullable(Float64),
    lower              Nullable(Float64),
    upper              Nullable(Float64),
    score              Float64,
    threshold          Float64,
    is_anomaly         Bool,
    direction          Int8,
    details            Map(LowCardinality(String), Float64),
    detected_at        DateTime64(3, 'UTC'),
    ingested_at        DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine_id, metric_name, algorithm, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
