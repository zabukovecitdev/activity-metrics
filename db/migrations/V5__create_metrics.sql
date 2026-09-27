-- Long format: one row per metric sample, matching core.metrics.ProcessedMetric.
-- Replaces the wide raw_metrics table, which is left in place (and ages out
-- via its retention policy) so this migration is non-destructive.
CREATE TABLE IF NOT EXISTS metrics (
    machine_id    TEXT               NOT NULL,
    name          TEXT               NOT NULL,
    "timestamp"   TIMESTAMPTZ        NOT NULL,
    type          TEXT               NOT NULL,
    unit          TEXT               NOT NULL,
    value         DOUBLE PRECISION   NOT NULL,
    -- Part of the key: the same metric name can carry several series at one
    -- instant (e.g. one per CPU core), distinguished only by attributes.
    attributes    JSONB              NOT NULL DEFAULT '{}'::jsonb,
    is_anomaly    BOOLEAN            NOT NULL DEFAULT FALSE,
    ingested_at   TIMESTAMPTZ        NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_id, name, attributes, "timestamp")
);

SELECT create_hypertable('metrics', by_range('timestamp'), if_not_exists => TRUE);
SELECT add_retention_policy('metrics', INTERVAL '24 hours', if_not_exists => TRUE);
