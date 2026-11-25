# 0013. 24 hours of samples and evaluations, no downsampling, machines kept forever

Status: Accepted

## Context

The first schema, on TimescaleDB, kept raw samples for 24 hours
(`add_retention_policy('raw_metrics', INTERVAL '24 hours')`, commit `85e17de`). The move to ClickHouse in commit
`5834725` kept the same 24 hours as a TTL, and `evaluations` got the same TTL when it was added.

Both dashboards open on the last 30 minutes. The detectors keep their history in Flink state, the last 360 values per
series ([ADR 0003](0003-count-based-window.md)), and never read ClickHouse.

An estimate of the volume: a collector round takes the 1-second gap plus the scrape, at least 100 ms for
`psutil.cpu_percent`, so about 78,000 rounds a day. Each round stores four samples per agent in `metrics` and four
evaluations per agent in `evaluations` ([ADR 0009](0009-an-evaluation-for-every-scored-sample.md)), so about 310,000
rows a day per machine in each table.

## Decision

- `metrics` and `evaluations` have `TTL toDateTime(timestamp) + INTERVAL 24 HOUR` with `ttl_only_drop_parts = 1`
  (`db/migrations/000001_create_tables.up.sql`, and `000002_rename_timestamp_columns.up.sql` for `metrics`). With
  daily partitions a row lives up to about 48 hours ([ADR 0005](0005-daily-partitions.md)).
- Nothing is downsampled. There is no rollup table and no materialized view.
- `machines` has no TTL. `ReplacingMergeTree(last_seen_at)` keeps one row per machine after a merge, so the table grows
  only with the number of machines. A machine that stops reporting keeps its row, and `last_seen_at` says when it was
  last seen.

## Consequences

- At most about 620,000 rows per machine in each of `metrics` and `evaluations`, by the estimate above. That is a small
  table for ClickHouse.
- Nothing older than a day can be charted or compared. "Is today's CPU unusual for a Monday?" has no data to answer it.
- A NAB replay with historical timestamps is dropped at the next TTL merge, so its evaluations do not stay around to
  study.
- A new consumer group that starts at `earliest` can write rows older than 24 hours from Kafka. The TTL drops them
  again.
- `machines` keeps a row for every machine that ever reported, including test machines.

What would change it:

- Wanting more than a day of history, such as a week-over-week comparison, would add a rollup. A materialized view
  from `metrics` into an `AggregatingMergeTree` table with min, average and max per 5 minutes, with its own longer TTL,
  keeps the raw table small.
- Wanting to keep NAB results would give them their own table or a longer TTL for `nab.` series.
- Machines that come and go, such as agents in short-lived containers, each with a new `machine_id`, would make
  `machines` grow without bound. A TTL on `last_seen_at`, for example 30 days, would fix that.
