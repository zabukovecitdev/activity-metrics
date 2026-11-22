# 0008. Daily partitions and 24-hour retention

Status: Accepted

Source: commit 8f3bb7c, README.md (Database), docs/architecture.md (Data model, Not doing yet), docs/simplification-plan.md (question 10).

## Context

Hourly partitions on the event timestamp split a replay with historical timestamps (the NAB benchmark) into one part per hour it touched. 38k rows made 16k parts and ClickHouse ran out of memory (8f3bb7c).

## Decision

Tables are partitioned by day. The partition key cannot be altered, so the seven migrations at that time were replaced by one that creates the final shape (8f3bb7c). `metrics` and `evaluations` keep 24 hours (TTL, `ttl_only_drop_parts`); `machines` has no TTL. Why 24 hours: the plan says "home-lab scale"; long-term storage and downsampling are listed as not done.

## Consequences

- A 14-day series fills 14 partitions instead of about 336 (8f3bb7c).
- With whole-part drops a row lives up to about 48 hours (README.md).
- Anything older than the TTL is gone; there is no downsampling.
