# 0004. MAD and EWMA, one evaluation per sample per algorithm

Status: Accepted

Source: commits 97b3c76, c51d470, 2bc661f; README.md (Detectors, Evaluation record); docs/architecture.md (Data model); docs/simplification-plan.md (question 8).

## Context

Grafana should draw each algorithm's baseline and band next to the raw series. That needs the calculation for every scored sample, not only the anomalies. The earlier design published only anomalies, on an `anomalies` topic (2bc661f).

## Decision

The job publishes one evaluation per scored sample per detector, anomalous or not, on `evaluations`, linked to its sample by `metric_id` (97b3c76). A `Detector` protocol makes a new algorithm a new class with no schema change. Two exist: MAD (robust, flags both directions) and EWMA (tracks drift, flags rises only). The plan's wording of why both: "different failure modes ... seeing both on the dashboard is the feature" (docs/simplification-plan.md).

## Consequences

- Rows double: two per scored sample. The table keeps only what the dashboard draws: baseline, lower, upper, is_anomaly (c51d470).
- The score and threshold stay inside the detector; only the flag is published (README.md).
- A detector without a band returns `None`, stored as `NULL` and shown as a gap.
- Detector correctness is not measured against labelled data; the floors are tuned by eye (docs/simplification-plan.md, question 7).
