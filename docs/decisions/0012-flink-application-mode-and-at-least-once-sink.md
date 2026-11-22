# 0012. Flink Application Mode, 1-second checkpoints, at-least-once sink

Status: Accepted

Source: commits 6d5d41f, b09b2eb, 31b4390 (checkpointing); docs/operations.md (Flink job); docs/simplification-plan.md (questions 3, 14).

## Context

The job was started by a separate submitter service with a script. Detector state should survive a restart (31b4390).

## Decision

The jobmanager starts the job itself with `standalone-job -pym activityreporter.anomaly_detector.main` (6d5d41f), so the submitter service and `submit-jobs.sh` are gone. Checkpoints are exactly-once, every second, written to a volume shared by the jobmanager and taskmanager and retained on cancellation (31b4390). The Kafka evaluations sink is set to `AT_LEAST_ONCE` explicitly (b09b2eb).

## Consequences

- A code change is picked up by `docker compose restart jobmanager`.
- A restart starts a new job: windows are empty and `raw_metrics` is read from `earliest`; replayed evaluations collapse in ClickHouse (docs/operations.md).
- Whether a 1-second interval is justified: the plan says "probably too aggressive" and leaves the choice open. Not recorded as decided.
- Idle series keep Flink state; there is no state TTL (plan, question 13).
