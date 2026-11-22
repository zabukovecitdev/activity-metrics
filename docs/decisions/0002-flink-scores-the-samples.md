# 0002. Flink scores the samples

Status: Accepted

Source: docs/architecture.md (Flink job row), docs/simplification-plan.md (question 2), commit e20cfb1.

## Context

Anomaly scoring needs the recent history of each series. docs/architecture.md says outright that "a plain Kafka consumer would do at this scale".

## Decision

A PyFlink job reads `raw_metrics`, keys the stream by `(machine_id, name)` so each series keeps its own state, and emits evaluations. It replaced a placeholder Flink job in e20cfb1. Why Flink rather than a consumer: it is a learning goal (docs/architecture.md).

## Consequences

- Per-series state, event time and checkpoints come from the framework.
- The Flink image runs Python 3.10 and PyFlink 1.19.1, so code under `anomaly_detector/` and `contracts/` avoids newer syntax (README.md, docs/operations.md).
- Only `anomaly_detector/flink/` and `main.py` import PyFlink; scoring logic is plain Python and tested in the regular environment (README.md).
- The break-even point for Flink over a consumer is a note in docs/simplification-plan.md (thousands of series, windows too big for memory). It is advice, not a measured result.
