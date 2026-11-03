CREATE TABLE metrics_old (
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
ORDER BY (machine_id, name, timestamp, metric_id);

INSERT INTO metrics_old (metric_id, machine_id, name, timestamp, type, unit, value, attributes, inserted_at)
SELECT metric_id, machine_id, name, timestamp, type, unit, value, attributes, pipeline_end FROM metrics;

RENAME TABLE metrics TO metrics_new, metrics_old TO metrics;

DROP TABLE metrics_new;
