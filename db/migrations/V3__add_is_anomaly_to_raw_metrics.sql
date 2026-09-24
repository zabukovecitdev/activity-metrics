-- Set by the anomaly detection Flink job; rows written before it existed are
-- not anomalies as far as we know.
ALTER TABLE raw_metrics ADD COLUMN is_anomaly BOOLEAN NOT NULL DEFAULT FALSE;
