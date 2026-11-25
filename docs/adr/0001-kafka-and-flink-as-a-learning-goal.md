# 0001. Kafka and Flink for a few machines, as a learning goal

Status: Accepted

## Context

activity-metrics watches a few home machines. Each agent reports four gauges (`system.cpu.utilization`,
`system.memory.usage`, `system.memory.limit`, `system.uptime`), and two of them are scored. At the collector's default
1-second gap that is four samples per agent about every second.

The production answer to "collect host metrics, flag anomalies, chart them" is Prometheus with node_exporter, alert
rules and Grafana. The compose file already runs Prometheus, for the pipeline's own counters. A plain Kafka consumer, or
one Python process that scrapes, scores and inserts, would also handle this load.

## Decision

- The pipeline uses Kafka (`raw_metrics`, `machines`, `evaluations`), a PyFlink job (`anomaly_detector/main.py`) and
  ClickHouse anyway. Learning how they behave is the goal of the project. This is said here so a reader does not have to
  guess.
- Each stage can be stopped, replayed or changed on its own: the collector only publishes, the Flink job only reads
  `raw_metrics` and writes `evaluations`, and `clickhouse-writer` only loads topics into tables.
- Prometheus stays for what it is good at: the counters in `shared/observability/metrics.py` and Flink's own metrics.

## When they become worth it

The window state is 360 doubles per series ([ADR 0003](0003-count-based-window.md)), about 3 KB. 10,000 series is about
30 MB, which one Python process holds. State size alone does not justify Flink at any size this project will reach.

Kafka earns its place when several independent readers need the same stream and its history: here the Flink job and the
metrics sink both read `raw_metrics` with their own groups, and a new reader can start from `earliest`. It also lets the
collector keep publishing while the writer or ClickHouse is down.

Flink earns its place when scoring no longer fits one process: tens of thousands of series at second resolution, a
detector that costs more than a sort of 360 values, or state that has to survive restarts and rescaling without losing
windows. The project has not measured where a single process runs out, so these are orders of magnitude, not a benchmark.

## Consequences

- `make up` starts nine containers for what one process could do. ClickHouse alone is allowed 2 GB of memory.
- The Flink job is the only part that cannot be unit tested in the `uv` environment
  ([ADR 0006](0006-python-3-10-in-the-flink-image.md)).
- Replacing Kafka and Flink with one process would remove most of `docker-compose.yml`, but not the detectors: they live
  in `anomaly_detector/algorithms/` and `anomaly_detector/services/`, free of PyFlink.
