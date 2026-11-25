# 0004. At-least-once delivery, duplicates collapsed by ReplacingMergeTree

Status: Accepted

## Context

A sample passes three hops after the collector: Kafka to the Flink job, the Flink job to the `evaluations` topic, and
each topic to its ClickHouse table. Any of them can fail between doing the work and recording that it was done.

`anomaly_detector/main.py` sets the checkpointing mode to `EXACTLY_ONCE`. That covers Flink's own state: the window
lists and the source offsets. It says nothing about what the Kafka sink has already written. The sink used to set no
delivery guarantee at all; commit `b09b2eb` made it explicit.

## Decision

- **Collector.** `KafkaRecordsProducer` sends with `acks=all` and `retries=3` and waits up to 10 seconds for each send.
  A publish that still fails is logged, counted as `publish_error`, and those samples are dropped. The next round
  produces new ones.
- **Flink job.** Checkpoints run every second (`enable_checkpointing(1000)`), in `EXACTLY_ONCE` mode, at least 500 ms
  apart, with a 60-second timeout. Compose sets `state.checkpoints.dir` to `file:///checkpoints` on the
  `flink_checkpoints` volume and retains checkpoints on cancellation. Without that setting Flink keeps checkpoints on the
  JobManager heap.
- **Kafka sink.** `KafkaEvaluationsSink` uses `DeliveryGuarantee.AT_LEAST_ONCE`: it flushes to Kafka on every
  checkpoint, so a failure can repeat evaluations but not lose them.
- **ClickHouse writer.** `ClickHouseSinkBase._flush` inserts a batch and only then commits the consumer's offsets.
  Auto-commit is off. A crash between the two replays the batch.
- **Deduplication in ClickHouse.** `metrics`, `evaluations` and `machines` are `ReplacingMergeTree` tables. A replayed row
  has the same sorting key as the first copy and collapses on merge. Every sample and evaluation carries its `metric_id`
  from the collector on, so a replay keeps its key ([ADR 0007](0007-metric-id-as-sample-identity.md)).

Exactly-once end to end would need a transactional Kafka sink (`EXACTLY_ONCE` with a transactional id prefix),
`read_committed` consumers in the writer, and idempotent inserts. It is not worth it while ClickHouse deduplicates anyway.

## Consequences

- Duplicates are visible until ClickHouse merges the parts. Queries that count rows read with `FINAL`.
- `docker compose restart jobmanager` starts a new job with no savepoint. The windows are empty and the source reads
  `raw_metrics` from `earliest` again. The repeated evaluations collapse like any other replay.
- A new consumer group starts at `earliest` and can rewrite what Kafka still holds. That is safe for the same reason.
- The collector is at-most-once for a failed publish. Samples lost there are not retried.
- One-second checkpoints bound how much a failure repeats. They also cost a checkpoint every second for a job this small.
  The interval was not tuned.
