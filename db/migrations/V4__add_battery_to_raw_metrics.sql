-- Nullable: machines without a battery report NULL, and rows written before
-- this migration have no battery data.
ALTER TABLE raw_metrics
    ADD COLUMN battery_charging   BOOLEAN,
    ADD COLUMN battery_percentage DOUBLE PRECISION;
