ALTER TABLE metrics MODIFY TTL toDateTime(timestamp) + INTERVAL 24 HOUR;
ALTER TABLE evaluations MODIFY TTL toDateTime(timestamp) + INTERVAL 24 HOUR;
