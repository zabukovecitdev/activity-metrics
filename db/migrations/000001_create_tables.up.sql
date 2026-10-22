-- Raw samples as devices report them.
-- Partitioned by day, not by hour: a batch with historical timestamps (a NAB replay
-- spans weeks) splits into one part per partition it touches, and ClickHouse never
-- merges parts across partitions. Hourly partitions made 16k parts from 38k rows
-- and ran the server out of memory.
-- ttl_only_drop_parts drops whole days, so a row can live up to about 48 hours.
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

-- Keeps only what the dashboard draws: each algorithm's line, band and anomaly flag.
-- metric_id is the sample's id in metrics, which keeps its value.
-- Anomalous samples are evaluations WHERE is_anomaly.
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

-- Latest description of each machine, from the agent's /v1/machine, matching
-- shared.machines.Machine. Joined to metrics and evaluations on machine_id.
-- observed_at is when the agent last described itself, so it doubles as
-- "last seen". No TTL, a machine that goes away keeps its row.
CREATE TABLE IF NOT EXISTS machines (
    machine_id    String,
    hostname      String,
    os            LowCardinality(String),
    os_version    LowCardinality(String),
    architecture  LowCardinality(String),
    cores         UInt16,
    total_memory  UInt64,
    total_disk    UInt64,
    last_boot     DateTime64(3, 'UTC'),
    observed_at   DateTime64(3, 'UTC'),
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY machine_id;
