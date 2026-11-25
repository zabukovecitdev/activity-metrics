# 0012. Machine info goes through Kafka, not straight from the collector to ClickHouse

Status: Accepted

## Context

The collector fetches each agent's `/v1/machine` on its first scrape and every `COLLECTOR_MACHINE_REFRESH_SECONDS`
(default 300) after that ([ADR 0002](0002-pull-based-collection.md)). That is about 12 records an hour per machine. The
`machines` table holds the latest one per `machine_id`.

At that rate the collector could insert into ClickHouse itself and skip a topic, a sink and a consumer group. Commit
`97b3c76` added the `machines` topic and a writer for it instead.

## Decision

- `Collector._refresh_machine` publishes a `Machine` to the `machines` topic through `KafkaRecordsProducer`, keyed by
  `machine_id`, the same way samples go to `raw_metrics`. An invalid response goes to `machines_dlq`.
- `ClickHouseMachinesSink` in `clickhouse-writer` loads the topic into `machines`, with consumer group
  `clickhouse-machines-writer`.
- The collector has no ClickHouse client, settings or credentials.

## Consequences

- **One write path.** Every row in ClickHouse comes from `clickhouse-writer`. Insert-then-commit, the retries, the
  handling of malformed records and the Prometheus counters apply to `machines` as to the other two tables.
- **The collector does not wait for ClickHouse.** It depends only on Kafka in Compose. While ClickHouse or the writer is
  down, machine info waits in the topic like the samples do.
- **Replay.** The topic keeps every description the collector published for as long as the broker keeps it. A new
  consumer group, or offsets reset to `earliest`, rebuilds `machines` from it, and `ReplacingMergeTree(last_seen_at)`
  keeps the newest row per machine.
- Kafka has no volume in Compose, so the replay only reaches back to the last `make up`. The table, which has no TTL, is
  the long-term record.
- One more topic, sink and consumer group for a dozen records an hour per machine.
