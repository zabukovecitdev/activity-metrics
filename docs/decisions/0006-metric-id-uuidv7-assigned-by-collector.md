# 0006. metric_id is a UUIDv7 assigned by the collector

Status: Accepted

Source: commit 2bc661f, README.md (Collector contract, Database), docs/simplification-plan.md (question 9, Phase 5A).

## Context

Metrics and anomalies were stored by separate writers and needed a join key (2bc661f). A sample also needs an identity that is unique even for two series at the same instant that differ only in `attributes` (README.md).

## Decision

The collector gives each sample its own UUIDv7 `metric_id`; the agent sends none. It is the sample's identity in Kafka, in `metrics`, and in its `evaluations`, and is the last part of each table's sorting and deduplication key (2bc661f, README.md).

## Consequences

- Metrics and evaluations join on `metric_id` alone.
- The id is minted per collector. Two collectors scraping one agent would store the same reading twice under different ids. With one collector this does not happen (README.md).
- The plan suggests a deterministic id from `(machine_id, name, timestamp)` as the fix; it is not done (docs/simplification-plan.md, Phase 5A).
- Why UUIDv7 over another id scheme: Not recorded.
