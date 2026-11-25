# 0011. A custom writer instead of ClickHouse's Kafka table engine

Status: Accepted

## Context

ClickHouse can read Kafka itself. A table with `ENGINE = Kafka` consumes a topic, and a materialized view inserts what
it reads into a `MergeTree` table. Settings such as `kafka_max_block_size`, `kafka_flush_interval_ms`,
`kafka_skip_broken_messages` and `kafka_handle_error_mode` control the batches and what happens to a record that does
not parse. That would replace the `clickhouse-writer` service with three migrations.

This repository never tried the Kafka engine. The comparison below is with its documented behaviour.

## Decision

`clickhouse-writer` is a Python process ([ADR 0010](0010-a-consumer-and-client-per-sink.md)):

- **Batching.** `ClickHouseSinkBase.run` inserts when it holds `WRITER_BATCH_SIZE` rows (default 100) or after
  `WRITER_BATCH_TIMEOUT_SECONDS` (default 5), and commits the offsets after the insert.
- **Retries.** `ClickHouseRepository.insert_batch` tries five times (`max_retries`), waiting 1, 2, 4 and 8 seconds
  between tries, logs each failure, and then raises so Compose restarts the process.
- **Malformed records.** `ClickHouseSinkBase._parse` validates each record with the same pydantic models the producers
  use (`Metric`, `Evaluation`, `Machine` in `shared/models/`). A record that fails is logged with its topic, partition
  and offset, counted in `MALFORMED`, and skipped. It never fails its batch.
- **Tests.** The sinks, the consumer wrapper, the repository and `main` have unit tests under `tests/clickhouse_writer/`
  that run without Kafka or ClickHouse.

## Consequences

- The writer owns about 380 lines of code in `clickhouse_writer/` and about as many lines of tests. Insert-then-commit,
  the retries and the shutdown are code to get right, not a ClickHouse setting.
- It is one more service to run, with its own failure modes, such as the exit after five failed inserts.
- Validation rules live once, in `shared/models/`. With the Kafka engine they would be SQL in a materialized view, next
  to a copy of the same rules in Python for the collector.
- Errors show up in `docker compose logs clickhouse-writer` and in Prometheus, not in the ClickHouse server log and
  `system.kafka_consumers`.
- The broker address and topic names stay in the `x-topics` block of `docker-compose.yml`, not in migrations, and
  `migrate` needs only ClickHouse.
- The Kafka engine becomes the better choice if the writer turns into a plain copy with nothing to validate, or if its
  code starts to cost more than it teaches.
