# 0009. Every scored sample produces an evaluation, anomalous or not

Status: Accepted

## Context

The Flink job first published only anomalies, to an `anomalies` topic and table. The dashboard could then mark the
flagged points, but not draw the baseline and bands they were flagged against, so a reader could not see why a point was
flagged or how close the others came.

## Decision

- `MetricEvaluator.evaluate` returns one `Evaluation` per detector for every scored sample, anomalous or not, once that
  detector has `min_points` values. `AnomalyDetectionFunction` emits all of them on `evaluations` (commit `97b3c76`).
- An evaluation holds only what the dashboard draws: `baseline`, `lower`, `upper` and `is_anomaly`, plus the sample's
  `metric_id`, `machine_id`, `metric_name`, `timestamp` and the `algorithm` (commit `c51d470`). The value stays in
  `metrics`. `lower` is `None` for EWMA, which has no lower band.
- `evaluations` replaced the `anomalies` table. Anomalous samples are `WHERE is_anomaly`.
- Unscored metrics (`system.memory.limit`, `system.uptime`) are dropped before any state is written, and a detector that
  is still warming up emits nothing for that series.

## Consequences

- `evaluations` gets one row per scored sample per algorithm: two per sample today, so four rows per agent per round for
  CPU and memory.
- The dashboard draws every algorithm's line and band next to the series, and its anomalies as points.
- A new algorithm is a new `algorithm` value, not a schema change.
- A sample without an evaluation (warm-up, unscored series) has no row to join. A query that joins `metrics` to
  `evaluations` needs `SETTINGS join_use_nulls = 1`, or ClickHouse fills those bands with `0` instead of `NULL`.
