# flink-jobs

PyFlink job scripts placed here are mounted into `/opt/flink/jobs` on both the
`jobmanager` and `taskmanager` containers.

Submit a job against the running session cluster with:

```bash
docker compose exec jobmanager flink run -py /opt/flink/jobs/<your_job>.py
```

The cluster's Kafka broker is reachable from inside the Flink containers at
`kafka:9092`. The Flink Web UI is available at http://localhost:8081.
