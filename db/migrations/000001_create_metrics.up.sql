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
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine_id, name, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
