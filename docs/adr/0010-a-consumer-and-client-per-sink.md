# 0010. One consumer and one ClickHouse client per sink, in one writer process

Status: Accepted

## Context

`clickhouse-writer` loads three topics into three tables: `raw_metrics` into `metrics`, `evaluations` into
`evaluations` and `machines` into `machines`. It has had three shapes:

- At commit `97b3c76` each table had its own Compose service and process: `metrics-writer`, `evaluations-writer` and
  `machines-writer`, each with one consumer group.
- Commit `6ef21a1` merged them into one process with one consumer on all three topics. It batched rows per table and
  committed only after every table in a flush was written, because the offsets belonged to the one consumer, not to a
  table. A failed insert into one table held back and replayed the other two.
- Commit `f3b630a` kept the one process and gave each table its own consumer again.

A `KafkaConsumer` from kafka-python and a clickhouse-connect `Client` are not safe to share between threads.

## Decision

- **One sink per topic.** `SINKS` in `clickhouse_writer/main.py` lists `ClickHouseMetricsSink`,
  `ClickHouseEvaluationsSink` and `ClickHouseMachinesSink`. Each gets its own `KafkaRecordsConsumer` and its own
  `ClickHouseRepository`, opened in one `ExitStack`.
- **One thread per sink.** A `ThreadPoolExecutor` runs each sink's `run` loop. The work is waiting on Kafka polls and
  HTTP inserts, so threads are enough.
- **Offsets and batches per topic.** Each sink has its own consumer group, `clickhouse-<table>-writer`
  (`ClickHouseSinkBase.group_id`), batches up to `WRITER_BATCH_SIZE` rows or `WRITER_BATCH_TIMEOUT_SECONDS`, and
  commits its offsets only after its own insert ([ADR 0004](0004-at-least-once-delivery.md)). A slow topic does not
  hold back the batches of a busy one.
- **All three stop together.** `main` waits with `FIRST_EXCEPTION`. When one sink raises, for example after
  `ClickHouseRepository.insert_batch` has failed five times, `stop_all` stops the other two. They flush what they
  hold and return, `future.result()` raises the error, and the process exits. Compose restarts it
  (`restart: unless-stopped`). `SIGTERM` calls the same `stop_all`.
- **Scaling is more processes.** A second `clickhouse-writer` joins the same three groups, and Kafka splits each topic's
  partitions between the processes.

## Consequences

- A malformed record, a batch, an offset and a log line belong to one table, so a problem is easy to place. The
  Prometheus counters in `shared/observability/metrics.py` are labelled by table.
- Failure isolation stops at the process. A ClickHouse error on one table restarts all three sinks. Only the failing
  sink has uncommitted rows to replay, so the other two lose nothing.
- Three consumers and three HTTP clients for one process, where one of each would do at this volume.
- One container, one image and one metrics port (`8001` on the host) instead of three.
- Compose does not create the topics. The broker creates them on first use with Kafka's default of one partition
  (`num.partitions`), so a second writer process would sit idle until the topics have more partitions.
- A new table is a new `ClickHouseSinkBase` subclass added to `SINKS`. It gets its own thread, consumer and group.
