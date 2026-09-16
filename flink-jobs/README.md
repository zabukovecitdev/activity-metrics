# flink-jobs

PyFlink job scripts placed here are mounted into `/opt/flink/jobs` on both the
`jobmanager` and `taskmanager` containers.

Submit a job against the running session cluster with:

```bash
docker compose exec jobmanager flink run -py /opt/flink/jobs/<your_job>.py
```

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
