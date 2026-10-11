# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse writers, and migrations. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volume `clickhouse_data` is kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |
| `8000` | Collector Prometheus endpoint (`/metrics`). |
| `8001` | ClickHouse writer Prometheus endpoint. The process binds `8000` inside the container. |
| `9090` | Prometheus UI. Targets are `collector:8000` and `clickhouse-writer:8000`. |
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

Every variable is parsed once in `shared/settings.py` (pydantic-settings). The name is the field with the class prefix: `KAFKA_`, `CLICKHOUSE_`, `WRITER_`. `AgentSettings.port` is `PORT`, with no prefix. Compose overrides are noted when they differ. A value of the wrong type exits at startup, and the validation error names the field (`port`, `batch_size`), not the environment variable.

### Agent

| Variable | Default | Compose |
| --- | --- | --- |
| `PORT` | `8080` | unset |

The dev-profile agent does not publish `8080`. `make agent` binds `0.0.0.0` on `PORT`.

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

One process runs a writer per topic, each on its own thread; the variables below apply to all three.

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `WRITER_BATCH_SIZE` | `100` | unset |
| `WRITER_BATCH_TIMEOUT_SECONDS` | `5.0` | unset |
| `CLICKHOUSE_HOST` | `localhost` | `clickhouse` |
| `CLICKHOUSE_PORT` | `8123` | `8123` |
| `CLICKHOUSE_DB` | `metrics` | `metrics` |
| `CLICKHOUSE_USER` | `user` | `user` |
| `CLICKHOUSE_PASSWORD` | `password` | `password` |

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

- **Code change:** `docker compose restart jobmanager`. The job starts again from the new code, with empty windows. See **State** below.
- **Job fails:** the `exponential-delay` restart strategy restarts it inside the cluster and restores the last checkpoint, windows included. If it fails for good, the jobmanager exits and Compose starts it again (`restart: unless-stopped`), which is a new submission and does not restore that checkpoint.
- **State:** `anomaly_detector/main.py` calls `enable_checkpointing(1000)` in `EXACTLY_ONCE` mode, with at least 500 ms between checkpoints and a 60 s timeout. Compose sets `state.checkpoints.dir=file:///checkpoints` on the jobmanager and the taskmanager (volume `flink_checkpoints`) and `execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION` on the jobmanager, so cancelled checkpoints stay on the volume. `make anomaly-detector` sets no checkpoint directory, so a local job keeps checkpoints on the JobManager heap.

The evaluations sink sets `DeliveryGuarantee.AT_LEAST_ONCE`. Flink requires checkpointing for that guarantee. On each checkpoint the sink waits until Kafka has acknowledged the outstanding records, and the Kafka source commits its offsets to the `anomaly-detector` group (the connector default; the job does not turn it off). A task restart while the jobmanager stays up restores window state and those source offsets. Evaluations written after the last completed checkpoint can be written again. The sink is not transactional. `ReplacingMergeTree` collapses the repeats on merge; until then, read with `FINAL`.

`docker compose restart jobmanager` does not load the files on `flink_checkpoints`. The command is `standalone-job` with no restore path, so the new job has empty windows. Its source is `KafkaOffsetsInitializer.earliest()`, which a fresh submission uses even when the `anomaly-detector` group already has committed offsets, so it rereads `raw_metrics` from the start of the log. `make up` also recreates Kafka, which has no volume, so that log is empty.

The taskmanager still runs as its own service and registers with the jobmanager at `jobmanager:6123`.

Every reader has its own consumer group, and each must see every record on its topic; two readers sharing a group would split the partitions between them. The writers use offset reset `earliest`, so a group that already has committed offsets resumes and a new group reads from the start of the log. The anomaly detector's `earliest()` initializer does not resume from the group offset; see **State** above.

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

## Packaging

The wheel contains `src/activityreporter`. Console scripts:

| Script | Entry |
| --- | --- |
| `agent` | `activityreporter.agent.main:cli` |
| `collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |

The root `Dockerfile` installs that wheel into one image, `activityreporter:latest`. Compose runs it for `agent` (`dev` profile only), `collector`, and `clickhouse-writer`, each with its console script as `command`. The anomaly detector runs in the Flink image instead.

## Monitoring

The collector and `clickhouse-writer` each call `start_http_server(8000)`. Compose publishes the collector on host `8000` and the writer on host `8001`. Prometheus `v3.5.0` listens on `9090` and scrapes `collector:8000` and `clickhouse-writer:8000` every 15 s (`docker/prometheus/prometheus.yml`). Grafana provisions a Prometheus datasource (uid `prometheus`, `http://prometheus:9090`). The dashboard queries ClickHouse only. `make smoke` does not check Prometheus.

`make collector` and `make clickhouse-writer` on the host at the same time both try to bind `8000`. Stop one, or run them in Compose.

| Metric | Labels | When it moves |
| --- | --- | --- |
| `activityreporter_collector_scrapes_total` | `outcome` = `ok`, `fetch_error`, or `publish_error` | One scrape of `/v1/metrics`. `fetch_error` is a transport error, a non-2xx response, or a body that fails validation. A `/v1/machine` fetch does not increment this counter. |
| `activityreporter_collector_invalid_responses_total` | `kind` = `metrics` or `machine` | `Collector._receive` got HTTP 200 and `model_validate_json` raised `ValidationError`. Still incremented when the dead-letter publish fails. |
| `activityreporter_collector_exceptions_total` | none | Ctrl-C in `collector/main.py`. `SIGTERM` logs `SIGTERM received, shutting down` and does not increment it. |
| `activityreporter_writer_rows_written_total` | `table` | After that writer's ClickHouse insert and Kafka offset commit. |
| `activityreporter_writer_malformed_records_total` | `table` | A record skipped in `_parse` before it joins a batch. |

Dead-letter topics are not in this table. An invalid body is counted above and published to `<topic>_dlq` (`raw_metrics_dlq`, `machines_dlq`), keyed by the agent URL, as `{"url", "error", "body"}`. Compose does not create those topics. The Kafka service leaves `auto.create.topics.enable` at the broker default (`true`), so the topic appears on the first successful dead-letter send. No consumer in this stack reads them. A transport error or a non-2xx response is not dead-lettered.

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and the dashboard respond. Each failure prints a hint.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

## Troubleshooting

**Collector exits with `Cannot load agents: ...`.** `collector.toml` is missing, isn't valid TOML, or `agents` isn't a non-empty list of `http://` or `https://` URLs; the message names which. Compose restarts the collector, so the line repeats in `docker compose logs collector` until the file is fixed. If the message says `collector.toml` is a directory, Docker created it for the mount because the file was missing: delete it and restore the file from git. On start the collector logs `Agents from collector.toml: [...]`.

**Scrape errors every 10 seconds.** A transport error or a non-2xx response is `Failed to fetch metrics from <url>`. A 200 body that is not a `MetricsResponse` is `Invalid metrics from <url>`: invalid JSON, an empty `machine_id`, a `timestamp` with no timezone offset, or a metric missing `name`, `type`, `unit`, or `value`. One bad metric dead-letters the whole body to `raw_metrics_dlq` and publishes no samples from that response. Both cases increment `activityreporter_collector_scrapes_total{outcome="fetch_error"}`; only the invalid body increments `activityreporter_collector_invalid_responses_total{kind="metrics"}`. An entry in `collector.toml` with a path, like `http://host:8080/v1/metrics`, still scrapes `/v1/metrics` at the root: list the base URL only.

**`Failed to dead-letter invalid metrics from <url>`** (or `machine`). The body was counted and then dropped; the scrape loop continues. Check `KAFKA_CONNECTION_STRING` (`localhost:9094` from the host, `kafka:9092` from Compose). To read a topic that does exist: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw_metrics_dlq --from-beginning`.

**Kafka send failures.** When `KafkaRawMetricsRepository.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` and the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host, `kafka:9092` from Compose services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check the writer logs and that its group is `clickhouse-metrics-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that.

**`Skipping malformed record` in a writer log.** The record is not a JSON object, or `from_json` / `to_row` raised `TypeError` or `ValueError`. Pydantic's `ValidationError` is a `ValueError`, so a `Metric` or `Machine` with an empty `machine_id`, a bad type, or a `last_boot` with no timezone offset is skipped here too. A metric or evaluation also needs a `metric_id` that `uuid.UUID` accepts, and an evaluation needs a non-empty `machine_id`, `metric_name`, and `algorithm`. Unknown JSON keys are ignored. The record is not retried. The same line increments `activityreporter_writer_malformed_records_total`.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing for a series until the last hour holds `min_points` samples of that `(machine_id, name)`: 11 for `ewma` and 20 for `mad`, about 2 and 3.5 minutes at the 10-second scrape interval. The series must be a gauge, and the name must not be one of `system.battery.charging`, `system.battery.utilization`, `system.memory.limit`, or `system.uptime`. Battery series are omitted entirely when the agent has no power supply. A job started before this change still publishes to `anomalies`; `make up` recreates the cluster with the new job.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 5 minutes after a successful publish. `Failed to fetch machine from ...` is a transport error or a non-2xx response. `Invalid machine from ...` is a 200 body that is not a `Machine`: a missing field (including `observed_at`), an empty `machine_id`, a negative count, or a `last_boot` with no timezone offset. That body is on `machines_dlq`. Either failure leaves the hostname missing and does not stop metrics. Check that `clickhouse-writer` is running.

**Prometheus target is down.** `http://localhost:9090/targets` should list `collector` and `clickhouse-writer` as UP. A down collector usually means it exited before `start_http_server`, most often `Cannot load agents` from a bad `collector.toml`, and Compose is restarting it. The writer target stays down while `migrate` has not exited 0, because the writer waits on that.

**`ValidationError` as the process starts.** The value does not match the field in `shared/settings.py`. The error names the field, so `PORT=abc` on the agent and `CLICKHOUSE_PORT=abc` on the writer both say `port`; the process that logged it tells them apart. `WRITER_BATCH_SIZE=abc` names `batch_size`. A numeric string is accepted: `CLICKHOUSE_PORT=8123` is the integer `8123`.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain, subject to the 24-hour TTL.
