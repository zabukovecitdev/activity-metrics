-- ingested_at is the time ClickHouse inserted the row (the default), so inserted_at says what it is.
-- observed_at is the time the agent described the machine, which the dashboard shows as "Last seen".
-- ClickHouse refuses to rename the version column of a ReplacingMergeTree, and both are one:
-- metrics is versioned by ingested_at, machines by observed_at. Both tables are rebuilt and copied.
-- Stop clickhouse-writer first: rows inserted between a copy and its swap are lost.
CREATE TABLE metrics_new (
    metric_id    UUID,
    machine_id   LowCardinality(String),
    name         LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    type         LowCardinality(String),
    unit         LowCardinality(String),
    value        Float64,
    attributes   Map(String, String),
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (machine_id, name, timestamp, metric_id)
TTL toDateTime(timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1;

INSERT INTO metrics_new (metric_id, machine_id, name, timestamp, type, unit, value, attributes, inserted_at)
SELECT metric_id, machine_id, name, timestamp, type, unit, value, attributes, ingested_at FROM metrics;

RENAME TABLE metrics TO metrics_old, metrics_new TO metrics;

DROP TABLE metrics_old;

CREATE TABLE machines_new (
    machine_id    String,
    hostname      String,
    os            LowCardinality(String),
    os_version    LowCardinality(String),
    architecture  LowCardinality(String),
    cores         UInt16,
    total_memory  UInt64,
    total_disk    UInt64,
    last_boot     DateTime64(3, 'UTC'),
    last_seen_at  DateTime64(3, 'UTC'),
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY machine_id;

INSERT INTO machines_new (machine_id, hostname, os, os_version, architecture, cores, total_memory, total_disk,
                          last_boot, last_seen_at, inserted_at)
SELECT machine_id, hostname, os, os_version, architecture, cores, total_memory, total_disk,
       last_boot, observed_at, ingested_at
FROM machines;

RENAME TABLE machines TO machines_old, machines_new TO machines;

DROP TABLE machines_old;
