-- Runs automatically on first start of an empty TimescaleDB data volume.
-- Later schema changes need a manual migration or a volume wipe.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS raw_metrics (
    machine_id    TEXT               NOT NULL,
    "timestamp"   TIMESTAMPTZ        NOT NULL,
    cpu_usage     DOUBLE PRECISION   NOT NULL,
    memory_usage  DOUBLE PRECISION   NOT NULL,
    memory_total  DOUBLE PRECISION   NOT NULL,
    labels        JSONB              NOT NULL DEFAULT '{}'::jsonb,
    ingested_at   TIMESTAMPTZ        NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_id, "timestamp")
);

-- The (machine_id, "timestamp") primary key already serves per-machine time-range
-- lookups in both scan directions, so no additional index is needed here.
SELECT create_hypertable('raw_metrics', by_range('timestamp'), if_not_exists => TRUE);
