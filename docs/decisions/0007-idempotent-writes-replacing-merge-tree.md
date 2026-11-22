# 0007. Safe replays: ReplacingMergeTree and commit after write

Status: Accepted

Source: commits 5834725, b09b2eb, 6ef21a1; README.md (ClickHouse sinks); docs/architecture.md (Not doing yet); docs/simplification-plan.md (question 3).

## Context

Kafka delivery to ClickHouse is at-least-once. The Kafka sink of the Flink job is `AT_LEAST_ONCE` (b09b2eb), and a failed insert replays its batch. ClickHouse has no `ON CONFLICT` (README.md).

## Decision

Tables are `ReplacingMergeTree`, so rows with the same sorting key collapse on merge (5834725). Each sink commits its Kafka offsets only after the insert succeeded; a failed insert leaves them uncommitted and the whole batch is replayed (6ef21a1, README.md). Reads use `FINAL` when duplicates matter. End-to-end exactly-once is on the "Not doing yet" list: it would need a transactional sink and `read_committed` consumers (docs/architecture.md).

## Consequences

- Replays after a failure or a Flink restart are safe; duplicates are visible until a merge runs.
- `metrics` and `evaluations` use `inserted_at` as the version column; `machines` uses `last_seen_at` (migration 000002).
- ClickHouse errors retry with exponential backoff, up to `CLICKHOUSE_MAX_RETRIES` (default 5).
- Flink's `EXACTLY_ONCE` checkpoint mode covers Flink's own state only (docs/architecture.md).
