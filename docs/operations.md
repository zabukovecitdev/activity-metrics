# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse writers, and migrations. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volume `clickhouse_data` is kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |
| `8000` | Collector process metrics, `GET /metrics`. |
| `8001` | ClickHouse writer process metrics. The container listens on `8000`; the host port is `8001`. |
| `9090` | Prometheus. |
| `3000` | Grafana. |

The `agent` Compose service is in the `dev` profile, so `make up` doesn't start it; `docker compose --profile dev up -d` does. It does not publish port `8080`. The collector scrapes the agents in `collector.toml`, mounted read-only at `/app/collector.toml` and read once at startup; after editing it, `docker compose restart collector`. The default entry, `http://host.docker.internal:8080`, is an agent run on the host with `make agent`. Docker Desktop resolves `host.docker.internal` by itself; on Docker Engine the collector's `extra_hosts: host.docker.internal:host-gateway` does it. The Compose agent is on the same network as the collector, so add `http://agent:8080` to scrape it instead.

To run the collector outside Docker (`make collector`), stop the Compose one first, or both scrape every agent and each sample is stored twice under different `metric_id`s. `host.docker.internal` doesn't resolve on the host, so list `http://localhost:8080` while you do.

Process commands from the `Makefile`:

| Target | Command | Entry module in `pyproject.toml` |
| --- | --- | --- |
| `agent` | `uv run agent` | `activityreporter.agent.main:cli` |
| `collector` | `uv run collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `uv run clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |
| `test` | `uv run pytest` | |
| `anomaly-detector` | `flink/.venv/bin/python -m activityreporter.anomaly_detector.main` | Passes `KAFKA_CONNECTOR_JAR` as an absolute path |

`anomaly-detector` creates `flink/.venv` with Python 3.11 and `apache-flink==1.19.1`, downloads `flink-sql-connector-kafka-3.2.0-1.19.jar` into `flink/lib/`, and exports that absolute path as `KAFKA_CONNECTOR_JAR`.

## Environment

Defaults are the field defaults on the settings classes in `shared/settings/`. Compose overrides are noted when they differ.

### Collector

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |

The agents to scrape are not an environment variable but `collector.toml`; see [Local stack](#local-stack) and the README.

### Anomaly detector

| Variable | Default | Compose (`jobmanager`) |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` |
| `KAFKA_CONNECTOR_JAR` | empty | unset (default applies) |

An empty `KAFKA_CONNECTOR_JAR` skips `pipeline.jars`. The Flink image already has that connector in `/opt/flink/lib`, and the jobmanager leaves the variable unset so it isn't loaded a second time. Local runs need the jar on the classpath because the `apache-flink` wheel does not include it. `make anomaly-detector` sets it; the job turns the path into a `file://` URI.

`docker/flink/Dockerfile` fetches the same jar (`3.2.0-1.19` on Flink `1.19.1`) and runs `chmod 644` on it. `ADD` from a URL otherwise leaves the file mode `600`, owned by root, and the `flink` user (uid 9999) cannot read it. The job then fails at submission with the connector classes unresolved.

The image installs PyFlink `apache-flink==1.19.1`, matching the base image. The job calls `env.set_python_executable(sys.executable)` so keyed Python operators use that interpreter. A worker started as plain `python` on `PATH` dies with `ModuleNotFoundError: pyflink`.

### ClickHouse writer

One process runs a sink per topic, each on its own thread; the variables below apply to all three.

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `WRITER_BATCH_SIZE` | `100` | unset |
| `WRITER_BATCH_TIMEOUT_SECONDS` | `5.0` | unset |
| `WRITER_POLL_TIMEOUT_MS` | `1000` | unset |
| `CLICKHOUSE_HOST` | `localhost` | `clickhouse` |
| `CLICKHOUSE_PORT` | `8123` | `8123` |
| `CLICKHOUSE_DB` | `metrics` | `metrics` |
| `CLICKHOUSE_USER` | `user` | `user` |
| `CLICKHOUSE_PASSWORD` | `password` | `password` |
| `CLICKHOUSE_MAX_RETRIES` | `5` | unset |
| `CLICKHOUSE_BACKOFF_BASE_SECONDS` | `1.0` | unset |

`CLICKHOUSE_MAX_RETRIES` must be greater than 0. `CLICKHOUSE_BACKOFF_BASE_SECONDS` may be 0. Both apply to every insert; the backoff sleeps sit inside the write-latency histogram below.

`CLICKHOUSE_PORT` is the HTTP port. From the host, ClickHouse is `localhost:8123` (HTTP) and `localhost:9000` (native) with the same database, user, and password. Inside Compose the writer waits until the `migrate` container exits successfully.

The topic names are set once, in the `x-topics` block of `docker-compose.yml`, and merged into every service that produces or consumes them. Services built from the root `Dockerfile` share the `x-app` block (`build`, `image`, `restart`).

## Migrations

The `migrate` service runs `migrate/migrate` with `up` on every `make up`, after the ClickHouse healthcheck passes. It connects over the native protocol (`clickhouse:9000`) with `x-multi-statement=true` (several statements per file) and `x-migrations-table-engine=MergeTree` (otherwise `schema_migrations` is a `TinyLog`). The URL lives once, in the service's `DATABASE_URL`; `docker compose run --rm migrate <args>` passes any CLI arguments through, which is what `make migrate`, `make migrate-down`, and `make migrate-new name=<name>` do.

Writing a migration:

- One DDL change per file, with `IF [NOT] EXISTS`, and a `.down.sql` that undoes it.
- ClickHouse DDL is not transactional. A file that fails halfway leaves some statements applied.
- Test `make migrate-down && make migrate` locally before committing.
- `x-multi-statement` splits on every `;`, including one inside a `--` comment, so keep semicolons out of comments.

**`Dirty database version N. Fix and force version.`** Migration `N` failed. `schema_migrations` marks it dirty and `migrate` refuses to run. Undo whatever part of `N` did apply (or finish it by hand), then record the real state and rerun:

```bash
docker compose run --rm migrate force <N-1>   # N's changes undone
docker compose run --rm migrate force <N>     # N's changes completed by hand
make migrate
```

## Flink job

The `jobmanager` service runs Flink in [Application Mode](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/overview/#application-mode): its command is `standalone-job -pym activityreporter.anomaly_detector.main -pyfs /opt/flink/src`, so the jobmanager starts the job's Python `main` itself, with `./src` mounted read-only at `/opt/flink/src`. `-pyfs` ships those files to the taskmanager with the job, so only the jobmanager mounts them. The Kafka variables are set on the jobmanager, because that is where `main` builds the source and sink.

The cluster exists for this one job. There is nothing to submit or cancel:

- **Code change:** `docker compose restart jobmanager`. The job starts again from the new code.
- **Job fails:** the `exponential-delay` restart strategy restarts it inside the cluster. If it fails for good, the jobmanager exits and Compose starts it again (`restart: unless-stopped`).
- **State:** `main` calls `enable_checkpointing(1000)` in `EXACTLY_ONCE` mode, with at least 500 ms between checkpoints and a 60 s timeout. Compose stores them at `file:///checkpoints` (volume `flink_checkpoints` on the jobmanager and the taskmanager) and sets `execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`. That covers Flink's window state and the source offsets. The Kafka sink is `AT_LEAST_ONCE`, so a restored job can repeat evaluations; `ReplacingMergeTree` collapses them. An in-cluster restart (the `exponential-delay` strategy) restores the latest checkpoint. `docker compose restart jobmanager` does not: the command has no savepoint path, so it starts a new job, the source reads `raw_metrics` from `earliest`, and the windows start empty. The repeated evaluations collapse on merge. Retained checkpoint files stay on the volume and are not loaded.

The taskmanager still runs as its own service and registers with the jobmanager at `jobmanager:6123`.

Every reader has its own consumer group. They all start at `earliest`, and each must see every record on its topic; two readers sharing a group would split the partitions between them.

| Process | Group id | Topic it reads |
| --- | --- | --- |
| ClickHouse writer (metrics) | `clickhouse-metrics-writer` | `raw_metrics` |
| ClickHouse writer (evaluations) | `clickhouse-evaluations-writer` | `evaluations` |
| ClickHouse writer (machines) | `clickhouse-machines-writer` | `machines` |
| Anomaly detector | `anomaly-detector` | `raw_metrics` |

The writers do not depend on the Flink job: `metrics` and `machines` fill even while the job is down.

A second `clickhouse-writer` process shares each of these groups, so Kafka splits every topic's partitions between the two; more processes than partitions leave some idle. The groups replaced the single group `clickhouse-writer`. A new group starts at `earliest` unless it still has committed offsets, so on a first run it can rewrite what Kafka still holds; `ReplacingMergeTree` collapses those rows.

`jobmanager` sets `restart-strategy.type: exponential-delay`. Without a restart strategy the job stops for good on the first error, including starting before the collector has created `raw_metrics`.

Task slots on the taskmanager: 2, matching the job parallelism.

## Process metrics

The collector and the clickhouse-writer each call `start_http_server(8000)` (`METRICS_PORT` in each `main.py`). Host ports are in the table above. Prometheus (`prom/prometheus:v3.5.0`) scrapes every 15 seconds, from `docker/prometheus/prometheus.yml`:

| Job | Target |
| --- | --- |
| `collector` | `collector:8000` |
| `clickhouse-writer` | `clickhouse-writer:8000` |
| `flink` | `jobmanager:9249`, `taskmanager:9249` |

Flink's `9249` is the pull reporter, set on both processes with `metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory` and `metrics.reporter.prom.port: 9249`. Compose leaves that port on the Compose network. The jobmanager and the taskmanager each listen on their own `9249`. Prometheus starts without waiting for either, so each `flink` target is down until that container answers.

`Metrics` in `shared/observability/metrics.py` registers every metric family when the class is imported, and both processes import it. A labelled sample is exported only after that process calls `.labels()`. The collector page carries the collector samples. The writer page carries the writer samples.

Both `make collector` and `make clickhouse-writer` bind host port `8000`. The Compose collector publishes that port, and the Compose writer publishes `8001`. Stop the Compose collector before either local command, or `start_http_server` fails. A local writer can listen while the Compose writer is up, because that container is on host port `8001`. The two writers then share each topic's consumer group, so Kafka splits the partitions between them.

| Series | Where it moves | Meaning |
| --- | --- | --- |
| `activityreporter_collector_messages_published_total{topic}` | collector, after `KafkaRecordsProducer.publish` or `dead_letter` | Records Kafka accepted. Labels are `raw_metrics`, `machines`, `raw_metrics_dlq`, and `machines_dlq`. The counter advances only when every send in that call succeeds, by the number of records in the call. A `KafkaError` leaves it unchanged, including for records in the same call whose sends already completed. |
| `activityreporter_collector_scrapes_total{outcome}` | collector, one metrics scrape | `ok`, `fetch_error` (transport error, HTTP error, or a body that fails validation), or `publish_error`. Machine fetches are not counted. |
| `activityreporter_collector_invalid_responses_total{kind}` | collector, before the dead-letter send | `metrics` or `machine`. A dead-letter send that fails is only the log line `Failed to dead-letter invalid ...`; this counter has already moved. |
| `activityreporter_writer_messages_consumed_total{table}` | writer, every poll | Records returned by the poll, including ones `_parse` then skips. The label is the ClickHouse table (`metrics`, `evaluations`, `machines`), not the topic. An empty poll adds 0. |
| `activityreporter_writer_rows_written_total{table}` | writer, after a flush returns | Rows in a flush whose insert and offset commit both finished. |
| `activityreporter_writer_malformed_records_total{table}` | writer, in `_parse` | Records logged as `Skipping malformed record` and dropped. |
| `activityreporter_writer_write_seconds{table}` | writer, around one flush | Histogram of `insert_batch` plus the offset commit, in seconds. `insert_batch` retries inside that interval, so the observation includes the backoff sleeps (`CLICKHOUSE_BACKOFF_BASE_SECONDS * 2^(attempt-1)`). Buckets are the `prometheus_client` defaults: 5 ms, 10 ms, 25 ms, 50 ms, 75 ms, 100 ms, 250 ms, 500 ms, 750 ms, 1 s, 2.5 s, 5 s, 7.5 s, 10 s, and `+Inf`. A flush that raises is still observed; its rows are not counted. The Prometheus names are `activityreporter_writer_write_seconds_bucket`, `_sum`, and `_count`. |

Flink adds its own series. Operator metrics exist on the taskmanager only while the job is running. The reporter exports Flink counters as Prometheus gauges (a Prometheus counter cannot go backwards). `rate()` still follows a rising gauge. A new job starts the gauge at 0, and `rate()` treats that drop as a reset.

| Series | Meaning |
| --- | --- |
| `flink_taskmanager_job_task_operator_evaluations_emitted` | One increment per evaluation row `AnomalyDetectionFunction` yields. A sample is scored once per detector that already has `min_points`, so a warm CPU or memory series emits two. Unscored names and a detector still warming up emit none. |
| `flink_taskmanager_job_task_operator_algorithm_scores{algorithm}` | Calls to `evaluate` that returned, labelled `mad` or `ewma`. |
| `flink_taskmanager_job_task_operator_algorithm_score_micros{algorithm}` | Sum of those calls, rounded to whole microseconds. A faster call adds 0. |
| `flink_taskmanager_job_task_operator_pendingRecords` | The Kafka connector's operator gauge `pendingRecords`: records past the consumer offset that the source has not fetched. |
| `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | Milliseconds per second the task spent back pressured. Near 1000 means the downstream task cannot keep up. |
| `flink_jobmanager_job_lastCheckpointDuration` | Milliseconds of the last completed checkpoint. Absent until the first one finishes. |

### Pipeline performance dashboard

`docker/grafana/dashboards/pipeline-performance.json` is provisioned as *Pipeline performance* (uid `pipeline-performance`). It uses the Prometheus datasource (uid `prometheus`). The range is `now-30m`, refresh is 10 s, and there are no variables. `make smoke` requests the machine-usage uid `ad8rn6h` only.

| Panel | What it graphs |
| --- | --- |
| Published in range, Published to Kafka /s | `activityreporter_collector_messages_published_total`, topics that do not end in `_dlq`. Flink writes `evaluations`; this counter counts the collector's topics. |
| Consumed by writer /s | `rate` of `activityreporter_writer_messages_consumed_total` by table. |
| ClickHouse rows written /s | `rate` of `activityreporter_writer_rows_written_total` by table. |
| Flink source lag (records) | `max(flink_taskmanager_job_task_operator_pendingRecords)`. |
| Flink evaluations emitted /s | `rate` of `evaluations_emitted`. |
| DLQ messages in range, DLQ messages /s | The published counter where `topic` matches `.*_dlq`. The stat is 0 when the series is absent. |
| Scrapes by outcome /s | `rate` of scrapes by `outcome`. |
| Malformed records skipped /s | `rate` of the malformed counter by table. |
| Detector score time (mean per call) | `rate(score_micros) / rate(scores)` by `algorithm`, in microseconds. The Python metric group exports those two counters, so the panel is a mean. |
| Detector scores /s | `rate` of `scores` by `algorithm`. |
| ClickHouse write time (p50 / p95) | `histogram_quantile` 0.5 and 0.95 of `activityreporter_writer_write_seconds_bucket` by table. The panel description is insert plus offset commit. |
| Backpressure (ms/s, max) | `max` of `backPressuredTimeMsPerSecond`. |
| Last checkpoint duration | `max` of `lastCheckpointDuration`, milliseconds. |

Published staying above consumed means the writer is behind on that topic. Consumed counts polls and rows written counts finished flushes, so the two differ while a batch is still open (`WRITER_BATCH_SIZE` or `WRITER_BATCH_TIMEOUT_SECONDS`) and by the malformed records. Write p95 includes retry sleeps, so a few failed inserts dominate it. An empty Flink panel means the `flink` target is down, or the job has not registered the operator yet.

## Packaging

The wheel contains `src/activityreporter`. Console scripts:

| Script | Entry |
| --- | --- |
| `agent` | `activityreporter.agent.main:cli` |
| `collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |

The root `Dockerfile` installs that wheel into one image, `activityreporter:latest`. Compose runs it for `agent` (`dev` profile only), `collector`, and `clickhouse-writer`, each with its console script as `command`. The anomaly detector runs in the Flink image instead.

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and the machine-usage dashboard (uid `ad8rn6h`) respond. Prometheus and *Pipeline performance* are outside that script. Each failure prints a hint.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

## Troubleshooting

**Collector exits with `Cannot load agents: ...`.** `collector.toml` is missing, isn't valid TOML, or `agents` isn't a non-empty list of `http://` or `https://` URLs; the message names which. Compose restarts the collector, so the line repeats in `docker compose logs collector` until the file is fixed. If the message says `collector.toml` is a directory, Docker created it for the mount because the file was missing: delete it and restore the file from git. On start the collector logs `Agents from collector.toml: [...]`.

**Scrape errors every 10 seconds.** The collector logs `Failed to fetch metrics from ...` for transport and HTTP errors, and `Invalid metrics from ...` for a 200 body that fails `MetricsResponse` validation. The same two lines exist for `machine`. An invalid body is counted in `activityreporter_collector_invalid_responses_total` and published to that topic's `_dlq`. An entry in `collector.toml` with a path, like `http://host:8080/v1/metrics`, still scrapes `/v1/metrics` at the root: `AgentUrls` joins an absolute path, so list the base URL only.

**Kafka send failures.** When `KafkaRecordsProducer.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` or `Failed to publish machine info from ... to Kafka` and that endpoint's scrape continues. The metrics scrape is counted as `publish_error`. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host, `kafka:9092` from Compose services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check the writer logs and that its group is `clickhouse-metrics-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/<CLICKHOUSE_MAX_RETRIES>)` and retried. After that many failures (default 5) the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that. The time spent retrying is one observation of `activityreporter_writer_write_seconds`, and `activityreporter_writer_rows_written_total` does not move for the failed flush.

**`Skipping malformed record` in a writer log.** The record is not valid JSON or lacks a required field. A metric or evaluation also needs a valid `metric_id`, an evaluation a non-empty `machine_id`, `metric_name`, and `algorithm`, and a machine a non-empty `machine_id` and a `last_boot` with a UTC offset. Records published before `metric_id` existed have none; they are skipped, not retried.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing for a series until the last hour holds `min_points` samples of that `(machine_id, name)`: 11 for `ewma` and 20 for `mad`, about 2 and 3.5 minutes at the 10-second scrape interval. Only `system.cpu.utilization` and `system.memory.usage` are scored. A job started before this change still publishes to `anomalies`; `make up` recreates the cluster with the new job.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 5 minutes after (`COLLECTOR_MACHINE_REFRESH_SECONDS`); check its log for `Failed to fetch machine from` or `Invalid machine from` (an agent older than this change has no `observed_at`), and that `clickhouse-writer` is running.

**Pipeline performance panels are empty or stuck at 0.** Open http://localhost:9090/targets. `collector` and `clickhouse-writer` should be up; `flink` is up only once the jobmanager and taskmanager answer on port `9249` inside the Compose network. Operator series appear after the job is running, and `lastCheckpointDuration` appears after the first checkpoint. Writer series are not on http://localhost:8000/metrics: that page is the collector, and a labelled series is exported only by the process that increments it. The writer's page is http://localhost:8001/metrics. `activityreporter_writer_messages_consumed_total` appears on the first poll, and may stay 0. `activityreporter_writer_write_seconds` appears on the first flush.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain, subject to the 24-hour TTL.
