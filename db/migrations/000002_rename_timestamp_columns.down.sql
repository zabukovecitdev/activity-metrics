CREATE TABLE metrics_old (
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

INSERT INTO metrics_old (metric_id, machine_id, name, timestamp, type, unit, value, attributes, ingested_at)
SELECT metric_id, machine_id, name, timestamp, type, unit, value, attributes, inserted_at FROM metrics;

RENAME TABLE metrics TO metrics_new, metrics_old TO metrics;

DROP TABLE metrics_new;

CREATE TABLE machines_old (
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

INSERT INTO machines_old (machine_id, hostname, os, os_version, architecture, cores, total_memory, total_disk,
                          last_boot, observed_at, ingested_at)
SELECT machine_id, hostname, os, os_version, architecture, cores, total_memory, total_disk,
       last_boot, last_seen_at, inserted_at
FROM machines;

RENAME TABLE machines TO machines_new, machines_old TO machines;

DROP TABLE machines_new;
