# 0005. Daily ClickHouse partitions, not hourly

Status: Accepted

## Context

`metrics` and `evaluations` keep 24 hours (`TTL toDateTime(timestamp) + INTERVAL 24 HOUR`) and used to be partitioned
by hour.

A NAB replay publishes series with historical timestamps that span weeks. ClickHouse splits an insert into one part per
partition it touches and never merges parts across partitions. With hourly partitions, 38k NAB rows made 16k parts and
ClickHouse ran out of memory (the Compose service is limited to 2 GB).

## Decision

- `metrics` and `evaluations` are partitioned by day: `PARTITION BY toYYYYMMDD(timestamp)` in
  `db/migrations/000001_create_tables.up.sql`. A 14-day series lands in 14 partitions instead of about 336.
- `ttl_only_drop_parts = 1` drops whole expired parts instead of rewriting them.
- A partition key cannot be altered, and every row expires after 24 hours, so the earlier migrations were squashed into
  one that creates the tables in their final shape (commit `8f3bb7c`).
- `machines` is small and has no TTL, so it has no partition key.

## Consequences

- With daily parts and a 24-hour TTL, a row lives up to about 48 hours.
- A replay of old data still creates one partition per day it spans. Those older than 24 hours go when ClickHouse next
  applies the TTL.
- The *Ingestion into ClickHouse* row of the *Metrics explorer* dashboard shows the parts per table. A count in the
  thousands means the partition key is too fine again.
