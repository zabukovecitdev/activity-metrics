# 0009. Dead-letter topics for invalid agent responses

Status: Accepted

Source: commit 72fb361, README.md (Collector contract, ClickHouse sinks), docs/simplification-plan.md (task 1.2).

## Context

An agent can return a body that does not match the contract. docs/simplification-plan.md (task 1.2) records an earlier half-built dead-letter field that nothing read, and recommended deleting it unless something would consume it.

## Decision

The collector validates `/v1/metrics` and `/v1/machine` in one place (`Collector._receive`) with the pydantic models. An invalid response is logged, counted in `activityreporter_collector_invalid_responses_total{kind}`, and published as received, with the URL and the error, to `<topic>_dlq` (`raw_metrics_dlq`, `machines_dlq`), keyed by agent URL (72fb361). One invalid metric sends the whole response there. A failed DLQ publish is logged and the scrape carries on.

## Consequences

- Nothing consumes the DLQ topics yet. What will read them: Not recorded.
- The ClickHouse writer has no DLQ. It converts records as it polls, logs `Skipping malformed record` and skips them, so a bad record cannot fail its batch on every retry (README.md, 2bc661f).
- A skipped record at the writer is not kept anywhere except the log.
