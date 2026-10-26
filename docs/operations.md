# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse writers, and migrations. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volume `clickhouse_data` is kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |
| `8000` | Collector `/metrics`. |
| `8001` | ClickHouse writer `/metrics`. The process listens on `8000`; Compose publishes that as `8001`. |
| `9090` | Prometheus. |
| `3000` | Grafana. |

Flink's Prometheus reporter listens on port `9249` in the jobmanager and the taskmanager. Compose does not publish it. Prometheus scrapes `jobmanager:9249` and `taskmanager:9249` on the Compose network.

The `agent` Compose service is in the `dev` profile, so `make up` doesn't start it; `docker compose --profile dev up -d` does. It does not publish port `8080`. The collector scrapes the agents in `collector.toml`, mounted read-only at `/app/collector.toml` and read once at startup; after editing it, `docker compose restart collector`. The default entry, `http://host.docker.internal:8080`, is an agent run on the host with `make agent`. Docker Desktop resolves `host.docker.internal` by itself; on Docker Engine the collector's `extra_hosts: host.docker.internal:host-gateway` does it. The Compose agent is on the same network as the collector, so add `http://agent:8080` to scrape it instead.

To run the collector outside Docker (`make collector`), stop the Compose one first, or both scrape every agent and each sample is stored twice under different `metric_id`s. `host.docker.internal` doesn't resolve on the host, so list `http://localhost:8080` while you do.

Process commands from the `Makefile`:

| Target | Command | Entry module in `pyproject.toml` |
| --- | --- | --- |
| `agent` | `uv run agent` | `activityreporter.agent.main:cli` |
| `collector` | `uv run collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `uv run clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |
| `test` | `uv run pytest` | |
| `mutate` | `uv run mutmut run` | dev dependency; see [Mutation testing](#mutation-testing) |
| `anomaly-detector` | `flink/.venv/bin/python -m activityreporter.anomaly_detector.main` | Passes `KAFKA_CONNECTOR_JAR` as an absolute path |

`anomaly-detector` creates `flink/.venv` with Python 3.11 and `apache-flink==1.19.1`, downloads `flink-sql-connector-kafka-3.2.0-1.19.jar` into `flink/lib/`, and exports that absolute path as `KAFKA_CONNECTOR_JAR`.

## Environment

Defaults are the `from_env` / `os.environ.get` fallbacks. Compose overrides are noted when they differ.

### Collector

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `COLLECTOR_SCRAPE_INTERVAL_SECONDS` | `1` | unset |
| `COLLECTOR_MACHINE_REFRESH_SECONDS` | `300` | unset |

`CollectorSettings` (`shared/settings/collector_settings.py`) reads those two with `env_prefix=COLLECTOR_` and `gt=0`. The scrape value is the sleep after a round finishes. The old names `SCRAPE_INTERVAL_SECONDS` and `MACHINE_REFRESH_SECONDS` are ignored. The agents to scrape are not an environment variable but `collector.toml`; see [Local stack](#local-stack) and the README.

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
- **State:** `anomaly_detector/main.py` calls `enable_checkpointing(1000)` in `EXACTLY_ONCE` mode, with a 500 ms minimum pause and a 60 s timeout. Compose sets `state.checkpoints.dir` to `file:///checkpoints` and retains files on cancellation (`RETAIN_ON_CANCELLATION`). The jobmanager command is `standalone-job` with no savepoint path, so `docker compose restart jobmanager` starts a new job: windows are empty and the Kafka source reads `raw_metrics` from `earliest` again. Replayed evaluations share a sorting key with the first copy and collapse in `ReplacingMergeTree`. A task failure while that job is still running restores its own checkpoint (`restart-strategy.type: exponential-delay`). The window list state is named `recent_values` and its type is a list of doubles (`WINDOW_POINTS`, 360). Checkpoints written before that change stored `(event time, value)` tuples under the same name. They remain on the volume. Resume a job only from a checkpoint written with the double list.

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

## Packaging

Console scripts, from `[project.scripts]`:

| Script | Entry |
| --- | --- |
| `agent` | `activityreporter.agent.main:cli` |
| `collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |

The root `Dockerfile` builds one image, `activityreporter:latest`, from `python:3.14-slim`. It copies `uv` 0.11.28 and the files `pyproject.toml`, `uv.lock`, `README.md`, and `src`, then runs:

```bash
UV_PYTHON_DOWNLOADS=never uv sync --frozen --no-default-groups --no-editable --no-cache
```

`--frozen` installs the versions in `uv.lock`. `--no-default-groups` skips `dev` and `experiments` (`[tool.uv] default-groups`). `--no-editable` installs the project so those console scripts land in `/app/.venv/bin`, which the image puts on `PATH`. `UV_PYTHON_DOWNLOADS=never` uses the image's Python 3.14. `README.md` is in that copy because `readme` in `pyproject.toml` names it. Compose runs this image for `agent` (`dev` profile only), `collector`, and `clickhouse-writer`, each with its console script as `command`. The anomaly detector runs in the Flink image instead.

## Process metrics

`Metrics` in `shared/observability/metrics.py` is the registry both processes export. Importing the class registers every family. The collector reaches it through `Collector` and `KafkaRecordsProducer`; the writer reaches it through `ClickHouseSinkBase`. The agent does not export metrics. Each of the other two calls `start_http_server(8000)` (`METRICS_PORT` in that app's `main.py`). On the host those are ports `8000` and `8001`. A labelled sample appears only after that process has called `.labels()` for it. Before that, `/metrics` has the `# HELP` and `# TYPE` lines and no sample for the family. `prometheus_client` 0.26.0, the version in `uv.lock`, also emits a `_created` gauge beside each labelled series.

| Name | Labels | When it moves |
| --- | --- | --- |
| `activityreporter_collector_scrapes_total` | `outcome`: `ok`, `fetch_error`, `publish_error` | One metrics scrape. `fetch_error` is a transport error, a non-2xx, or a body that fails `MetricsResponse`. `publish_error` is a `KafkaError` from `publish`. `ok` is a publish that returned. A machine fetch leaves this counter alone. |
| `activityreporter_collector_invalid_responses_total` | `kind`: `metrics` or `machine` | A 200 body that fails validation, counted before the dead-letter send. |
| `activityreporter_collector_messages_published_total` | `topic` | After every send in that `publish` call has succeeded, by the number of records. A dead-letter send adds 1 to the `<topic>_dlq` series after that send succeeds. |
| `activityreporter_writer_messages_consumed_total` | `table` | Every poll, by `len(records)`, malformed records included. An empty poll calls `inc(0)` and still creates the series at 0. |
| `activityreporter_writer_malformed_records_total` | `table` | `_parse` caught `TypeError` or `ValueError`. |
| `activityreporter_writer_rows_written_total` | `table` | After `insert_batch` and the offset commit have both returned. |
| `activityreporter_writer_write_seconds` | `table` | Histogram around that same insert and commit. Retry sleeps inside `insert_batch` are part of the observation. The timer records on an exception too; the row counter above stays where it was. |

`table` is `metrics`, `evaluations`, or `machines`. Histogram buckets are 5 ms, 10 ms, 25 ms, 50 ms, 75 ms, 100 ms, 250 ms, 500 ms, 750 ms, 1 s, 2.5 s, 5 s, 7.5 s, 10 s, and `+Inf`, plus `_bucket`, `_sum`, and `_count`.

Both `main.py` files hardcode port 8000. Compose can publish them on two host ports because they are different containers. Two processes on the host cannot: the second `start_http_server(8000)` fails to bind.

Prometheus is `prom/prometheus:v3.5.0` on port `9090`. `docker/prometheus/prometheus.yml` scrapes every 15 seconds:

| Job | Targets |
| --- | --- |
| `collector` | `collector:8000` |
| `clickhouse-writer` | `clickhouse-writer:8000` |
| `flink` | `jobmanager:9249`, `taskmanager:9249` |

The Prometheus service waits until the collector and the writer have started. It does not wait for Flink. Grafana's Prometheus datasource is uid `prometheus`, URL `http://prometheus:9090`. ClickHouse stays the default datasource.

The jobmanager and the taskmanager set `metrics.reporter.prom.factory.class` to `org.apache.flink.metrics.prometheus.PrometheusReporterFactory` and `metrics.reporter.prom.port` to `9249`. `AnomalyDetectionFunction` adds its own counters on the operator: `evaluations_emitted` once per evaluation it yields, and, under a group named `algorithm`, `scores` and `score_micros`. `scores` increments once per `Detector.evaluate` that actually ran. A detector still short of `min_points` is skipped, so warm-up does not count. `score_micros` adds `round(seconds * 1_000_000)`. The pipeline dashboard reads the reporter names `flink_taskmanager_job_task_operator_evaluations_emitted`, `flink_taskmanager_job_task_operator_algorithm_scores`, and `flink_taskmanager_job_task_operator_algorithm_score_micros` (label `algorithm`). Mean time per call is `rate(score_micros) / rate(scores)`. PyFlink's counters are not a histogram.

`docker/grafana/dashboards/pipeline-performance.json` is uid `pipeline-performance`, refresh 10 seconds, default range the last 30 minutes, timezone `browser`. The Metrics explorer link opens it and keeps the current time range (`keepTime`).

| Panel | What it shows |
| --- | --- |
| Collector, Flink, ClickHouse writer | `min(up{job="..."})`. 1 is UP. Flink's job has two targets, so one failed scrape makes the stat DOWN. A missing series is filled with 0. |
| Errors in range | Over the dashboard range: dead-letter publishes (`topic` matching `.*_dlq`), malformed writer records, and scrapes whose `outcome` is anything other than `ok`. |
| Collector: published to Kafka /s | `rate` of publishes, `*_dlq` topics left out, summed by topic. |
| Flink: evaluations emitted /s | `rate` of `evaluations_emitted`. |
| ClickHouse: rows written /s | `rate` of `rows_written_total` by table. |
| Flink: source lag | `max(flink_taskmanager_job_task_operator_pendingRecords)`. Records in `raw_metrics` the detector has not read yet. Growing means Flink is behind. |
| Flink: backpressure | `max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)`, milliseconds per second the busiest task was blocked. 1000 is fully blocked. |
| Flink: checkpoint duration | `max(flink_jobmanager_job_lastCheckpointDuration)`, milliseconds, the last checkpoint. The job checkpoints every second. |
| Flink: detector time per call | The microsecond ratio above, per algorithm. |
| ClickHouse: write time p50 / p95 | `histogram_quantile` 0.5 and 0.95 of `activityreporter_writer_write_seconds_bucket`, seconds, per table. One observation is one batch. |

## Mutation testing

`make mutate` runs `uv run mutmut run`. `[tool.mutmut]` in `pyproject.toml` mutates `src/activityreporter/anomaly_detector/algorithms/` and `services/`, then runs `tests/anomaly_detector/`. mutmut copies only the paths it mutates, so `also_copy` lists the rest of what those tests import: the package and detector `__init__.py` files, `anomaly_detector/models`, `shared`, and `tests`. A new import from anywhere else fails the mutant run with `ImportError` until that path is added there.

`mutmut` is in the `dev` dependency group. The app image installs with `--no-default-groups`, so run this on the host after `uv sync`. The `mutants/` directory is gitignored. `pytest` is configured with `--ignore=mutants`, so `make test` does not collect a leftover tree. `uv run mutmut results` lists what survived; `uv run mutmut show <id>` prints one mutant. The Flink process function is outside `source_paths` because it imports PyFlink, which is not installed in this environment.

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and dashboard uid `ad8rn6h` respond. The containers it requires are kafka, clickhouse, collector, clickhouse-writer, jobmanager, taskmanager, and grafana. Prometheus and dashboard uid `pipeline-performance` are outside that list. Each failure prints a hint.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

## Troubleshooting

**Collector exits with `Cannot load agents: ...`.** `collector.toml` is missing, isn't valid TOML, or `agents` isn't a non-empty list of `http://` or `https://` URLs; the message names which. Compose restarts the collector, so the line repeats in `docker compose logs collector` until the file is fixed. If the message says `collector.toml` is a directory, Docker created it for the mount because the file was missing: delete it and restore the file from git. On start the collector logs `Agents from collector.toml: [...]`.

**Scrape errors each round.** The collector logs `Failed to fetch metrics from ...` for transport and HTTP errors, and `Invalid metrics from ...` for a 200 body that fails `MetricsResponse` validation. The gap before the next round is `COLLECTOR_SCRAPE_INTERVAL_SECONDS` (default 1). An entry in `collector.toml` with a path, like `http://host:8080/v1/metrics`, still scrapes `/v1/metrics` at the root: list the base URL only.

**Kafka send failures.** When `KafkaRecordsProducer.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` and the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host, `kafka:9092` from Compose services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check the writer logs and that its group is `clickhouse-metrics-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that.

**`Skipping malformed record` in a writer log.** The record is not valid JSON or lacks a required field. A metric or evaluation also needs a valid `metric_id`, an evaluation a non-empty `machine_id`, `metric_name`, and `algorithm`, and a machine a non-empty `machine_id` and a `last_boot` with a UTC offset. Records published before `metric_id` existed have none; they are skipped, not retried.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing until that series' window holds `min_points` values: 11 for `ewma` and 20 for `mad`. At the default 1-second gap that is about 11 and 20 seconds; at a steady 10-second cadence, about 2 and 3.5 minutes; a 5-minute NAB series needs about 100 minutes before `mad` emits. Only `system.cpu.utilization`, `system.memory.usage`, and names that start with `nab.` are scored. A `docker compose restart jobmanager` starts empty and rereads `raw_metrics` from `earliest`, so the warm-up runs again.

**A flat `nab.` series flags every rise, with the upper band on the baseline.** Floorless EWMA (`EWMA(min_deviation=0.0)`) treats a scale of 0 as "any rise is an anomaly" and sets `upper` equal to `baseline`. On the detector explorer the dashed upper line sits on the average and the point is marked. A flat continuation and a drop are not anomalies. This used to raise `ZeroDivisionError` and restart the job. CPU and memory keep a positive floor, so a tiny step there stays inside the band.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 300 seconds after (`COLLECTOR_MACHINE_REFRESH_SECONDS`); check its log for `Failed to fetch machine from` or `Invalid machine from` (a body missing `last_seen_at`, or a `last_boot` without a UTC offset, fails validation), and that `clickhouse-writer` is running.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain, subject to the 24-hour TTL.

**Pipeline performance is empty, or a State stat stays DOWN.** Prometheus is at http://localhost:9090. From the host, the collector is http://localhost:8000/metrics and the writer is http://localhost:8001/metrics. Flink's reporter is only on the Compose network (`jobmanager:9249`, `taskmanager:9249`). The Flink stat is the minimum of both targets, so one of them failing the scrape shows DOWN. Right after `make up`, Prometheus may already be up while Flink is not: it waits on the collector and the writer, not on the jobmanager. A family with a `# TYPE` line and no sample has not been labelled on that process yet. `make smoke` can pass while this dashboard is empty.

**A local collector or writer fails at startup because port 8000 is taken.** Each calls `start_http_server(8000)`. The port is the constant `METRICS_PORT` in that `main.py`. The second process raises `OSError` (`Address already in use`). The Compose collector already publishes host port `8000`, so `make collector` while the stack is up hits the same error. The writer container is on host port `8001`.
