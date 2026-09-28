-- One row per metric sample, matching shared.metrics.ProcessedMetric.
-- The sorting key doubles as the deduplication key: a replayed Kafka batch
-- inserts the same rows again and ReplacingMergeTree collapses them on merge,
-- so read with FINAL when duplicates matter. Map can't be part of a sorting
-- key, so the series attributes are keyed by a hash of the sorted map.
CREATE TABLE IF NOT EXISTS metrics (
    machine_id   LowCardinality(String),
    name         LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    type         LowCardinality(String),
    unit         LowCardinality(String),
    value        Float64,
    attributes   Map(String, String),
    is_anomaly   Bool DEFAULT false,
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine_id, name, timestamp, cityHash64(mapSort(attributes)))
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;
