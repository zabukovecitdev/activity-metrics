# flink-jobs

PyFlink job scripts placed here are mounted into `/opt/flink/jobs` on both the
`jobmanager` and `taskmanager` containers.

`docker compose up` starts a one-shot `flink-jobs-submitter` service that
submits every `*.py` in this directory to the session cluster (detached), with
`src/` on the job's `PYTHONPATH` so jobs can import shared code such as
`core`. It skips submission if the cluster already has running jobs; to
resubmit after changing a job, cancel it in the Web UI and rerun:

```bash
docker compose up flink-jobs-submitter
```

`metrics_aggregator.py` is a minimal example: it wires a Kafka source table to
the `metrics` topic and prints every row via a `print` sink.

The cluster's Kafka broker is reachable from inside the Flink containers at
`kafka:9092`. The Flink Web UI is available at http://localhost:8081.

## Consumer groups

The `metrics` topic is consumed by two independent consumers, each of which must
receive every message. Pick a `group.id` for the Kafka source here that differs
from the one already taken by the `metrics-writer` service:

| Consumer | `group.id` |
| --- | --- |
| `metrics-writer` (raw metrics → TimescaleDB) | `timescale-writer` (reserved) |
| PyFlink aggregation job | e.g. `flink-metrics-aggregator` |
