# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse writers, and migrations. Behavior below is what the current source and Compose file do. See [reference](reference.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volume `clickhouse_data` is kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place. The ClickHouse service is limited to 2 GB of memory (a 512 MB reservation) and 262144 open files.

Published ports on the host:

| Port | Service |
| --- | --- |
| `3000` | Grafana, default login `admin` / `admin` |
| `8081` | Flink web UI |
| `8123`, `9000` | ClickHouse HTTP and native |
| `9094` | Kafka, for clients on the host |
| `8000` | Collector `GET /metrics` |
| `8001` | ClickHouse writer `GET /metrics`. The process listens on `8000`; Compose publishes it as `8001`. |
| `9090` | Prometheus. Its targets are at http://localhost:9090/targets. |

The agent's port `8080` and Flink's Prometheus reporter port `9249` are not published. `make agent` runs an agent on the host instead.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |

The `agent` Compose service is in the `dev` profile, so `make up` doesn't start it; `docker compose --profile dev up -d` does. It does not publish port `8080`. The collector scrapes the agents in `collector.toml`, mounted read-only at `/app/collector.toml` and read once at startup; after editing it, `docker compose restart collector`. The default entry, `http://host.docker.internal:8080`, is an agent run on the host with `make agent`. Docker Desktop resolves `host.docker.internal` by itself; on Docker Engine the collector's `extra_hosts: host.docker.internal:host-gateway` does it. The Compose agent is on the same network as the collector, so add `http://agent:8080` to scrape it instead.

To run the collector outside Docker (`make collector`), stop the Compose one first, or both scrape every agent and each sample is stored twice under different `metric_id`s. `host.docker.internal` doesn't resolve on the host, so list `http://localhost:8080` while you do.

Process commands from the `Makefile`:

| Target | Command | Entry module in `pyproject.toml` |
| --- | --- | --- |
| `agent` | `uv run agent` | `activity_metrics.agent.main:cli` |
| `collector` | `uv run collector` | `activity_metrics.collector.main:cli` |
| `clickhouse-writer` | `uv run clickhouse-writer` | `activity_metrics.clickhouse_writer.main:cli` |
| `test` | `uv run pytest` | |
| `mutate` | `uv run mutmut run` | dev dependency; see [Mutation testing](#mutation-testing) |
| `anomaly-detector` | `flink/.venv/bin/python -m activity_metrics.anomaly_detector.main` | Passes `KAFKA_CONNECTOR_JAR` as an absolute path |

`anomaly-detector` creates `flink/.venv` with Python 3.10, the version in the Flink image, installs `docker/flink/requirements.txt`, downloads `flink-sql-connector-kafka-3.2.0-1.19.jar` into `flink/lib/`, and exports that absolute path as `KAFKA_CONNECTOR_JAR`.

## Environment

Defaults are the pydantic-settings classes in `shared/settings/`. Compose overrides are noted when they differ.

### Agent

| Variable | Default | Compose |
| --- | --- | --- |
| `PORT` | `8080` | unset |

`AgentSettings` has no env prefix, and matching is case-insensitive, so `PORT` and `port` both set it. The agent listens on `0.0.0.0`. A value that is not an integer stops it with a `ValidationError`. `collector.toml` has to name the same port.

### Collector

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `COLLECTOR_SCRAPE_INTERVAL_SECONDS` | `1` | unset |
| `COLLECTOR_MACHINE_REFRESH_SECONDS` | `300` | unset |

`CollectorSettings` (`shared/settings/collector_settings.py`) reads those two with `env_prefix=COLLECTOR_` and `gt=0`. The scrape value is the sleep after a round finishes. The old names `SCRAPE_INTERVAL_SECONDS` and `MACHINE_REFRESH_SECONDS` are ignored. The agents to scrape are not an environment variable but `collector.toml`; see [Local stack](#local-stack) and the [reference](reference.md#agents).

### Anomaly detector

| Variable | Default | Compose (`jobmanager`) |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` |
| `KAFKA_CONNECTOR_JAR` | empty | unset (default applies) |

An empty `KAFKA_CONNECTOR_JAR` skips `pipeline.jars`. The Flink image already has that connector in `/opt/flink/lib`, and the jobmanager leaves the variable unset so it isn't loaded a second time. Local runs need the jar on the classpath because the `apache-flink` wheel does not include it. `make anomaly-detector` sets it; the job turns the path into a `file://` URI.

`docker/flink/Dockerfile` fetches the same jar (`3.2.0-1.19` on Flink `1.19.1`) and runs `chmod 644` on it. `ADD` from a URL otherwise leaves the file mode `600`, owned by root, and the `flink` user (uid 9999) cannot read it. The job then fails at submission with the connector classes unresolved.

The image installs the packages in `docker/flink/requirements.txt`: PyFlink `apache-flink==1.19.1`, matching the base image, and `pydantic-settings`, `pydantic` and `pydantic-core` at their versions in `uv.lock`. `make anomaly-detector` installs the same file into `flink/.venv`. The job calls `env.set_python_executable(sys.executable)` so keyed Python operators use that interpreter. A worker started as plain `python` on `PATH` dies with `ModuleNotFoundError: pyflink`.

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

`CLICKHOUSE_MAX_RETRIES` must be greater than 0. `CLICKHOUSE_BACKOFF_BASE_SECONDS` may be 0, which retries with no sleep. `WRITER_POLL_TIMEOUT_MS` must be greater than 0. `WRITER_BATCH_SIZE` and `WRITER_BATCH_TIMEOUT_SECONDS` have no lower bound; see the troubleshooting entries below for what 0 or less does.

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

The `jobmanager` service runs Flink in [Application Mode](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/overview/#application-mode): its command is `standalone-job -pym activity_metrics.anomaly_detector.main -pyfs /opt/flink/src`, so the jobmanager starts the job's Python `main` itself, with `./src` mounted read-only at `/opt/flink/src`. `-pyfs` ships those files to the taskmanager with the job, so only the jobmanager mounts them. The Kafka variables are set on the jobmanager, because that is where `main` builds the source and sink.

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

The wheel contains `src/activity_metrics`. Console scripts:

| Script | Entry |
| --- | --- |
| `agent` | `activity_metrics.agent.main:cli` |
| `collector` | `activity_metrics.collector.main:cli` |
| `clickhouse-writer` | `activity_metrics.clickhouse_writer.main:cli` |

The root `Dockerfile` builds one image, `activity-metrics:latest`, from `python:3.14-slim`. It copies `pyproject.toml`, `uv.lock`, `README.md` and `src`, then runs `uv sync --frozen --no-default-groups --no-editable --no-cache` with `UV_PYTHON_DOWNLOADS=never`. `--frozen` installs the versions in `uv.lock`, `--no-default-groups` leaves out the `dev` and `experiments` groups, and `--no-editable` puts the console scripts in `/app/.venv/bin`, which is on `PATH`. `README.md` is copied because `readme` in `pyproject.toml` names it. Compose runs it for `agent` (`dev` profile only), `collector`, and `clickhouse-writer`, each with its console script as `command`. The anomaly detector runs in the Flink image instead, `activity-metrics-flink:1.19.1`.

Upgrade note: the project used to be called `activityreporter`. The Prometheus metrics are now `activity_metrics_*` instead of `activityreporter_*`, and the images are `activity-metrics` and `activity-metrics-flink` instead of `activityreporter` and `activityreporter-flink`. Prometheus keeps the old series under their old names until they expire, and `docker image rm` removes the old images. The ClickHouse database and tables keep their names.

## Pipeline metrics

Prometheus (`prom/prometheus:v3.5.0`) scrapes `docker/prometheus/prometheus.yml` every 15 seconds:

| Job | Targets |
| --- | --- |
| `collector` | `collector:8000` |
| `clickhouse-writer` | `clickhouse-writer:8000` |
| `flink` | `jobmanager:9249`, `taskmanager:9249` |

The Prometheus service waits for the collector and the writer to start, not for Flink. Grafana's Prometheus datasource is `http://prometheus:9090` (uid `prometheus`); ClickHouse stays the default datasource. A collector or writer started with `make` on the host is not one of these targets.

`collector/main.py` and `clickhouse_writer/main.py` each call `start_http_server(8000)`. `METRICS_PORT` is a constant in each `main.py`, not a setting. In Compose the two processes are separate containers, so both can bind `8000`. On the host, the second one fails with `Address already in use`.

The Python series are defined in `shared/observability/metrics.py`:

| Series | Labels | When it moves |
| --- | --- | --- |
| `activity_metrics_collector_scrapes_total` | `outcome`: `ok`, `fetch_error`, `publish_error` | Once per agent per round, for the metrics fetch. `fetch_error` is a transport error, a non-2xx response, or a body that failed validation. `publish_error` is a Kafka send failure. A machine fetch does not change it. |
| `activity_metrics_collector_invalid_responses_total` | `kind`: `metrics` or `machine` | A body that failed validation, counted before the dead-letter send. |
| `activity_metrics_collector_messages_published_total` | `topic` | After a successful send, by the number of records. Dead-letter topics are included. |
| `activity_metrics_writer_messages_consumed_total` | `table` | Records returned by a poll, malformed ones included. |
| `activity_metrics_writer_malformed_records_total` | `table` | A record skipped as `Skipping malformed record`. It is not dead-lettered. |
| `activity_metrics_writer_rows_written_total` | `table` | After the insert and the offset commit both returned. |
| `activity_metrics_writer_write_seconds` | `table` | Histogram around that insert and commit, one observation per flush, with the client's default buckets (5 ms to 10 s). Retries and their backoff are inside one observation. |

`table` is `metrics`, `evaluations` or `machines`. A labelled series appears only after its first `.labels()` call; before that, `/metrics` has only the `# HELP` and `# TYPE` lines.

Both Flink processes load `PrometheusReporterFactory` on port `9249`. `AnomalyDetectionFunction.open` registers `evaluations_emitted`, incremented once per evaluation, and, under the group `algorithm`, `scores` and `score_micros`: one count per detector call that ran, and the microseconds it took. A detector still short of `min_points` adds nothing.

*Pipeline performance* (`docker/grafana/dashboards/pipeline-performance.json`, uid `pipeline-performance`) shows the last 30 minutes and refreshes every 10 seconds. The link at the top of *Metrics explorer* opens it with the same time range. Its rows:

- **State**: `min(up{job="..."})` for the collector, Flink and the writer. Flink has two targets, so one failed scrape shows DOWN. UP means the metrics port answered, not that agents are reachable.
- **Errors in range**: dead-letter publishes, malformed writer records, and scrapes whose outcome is not `ok`, as three numbers.
- **Throughput**: records published to Kafka, evaluations emitted, and rows written, per second.
- **Latency and saturation**: Flink source lag (`pendingRecords`, records not fetched yet), backpressure (milliseconds per second blocked), the last checkpoint duration, detector time per call, and the writer's p50 and p95 write time.

Evaluations per second stay below the collector's publish rate by design: an agent publishes four samples per round, two of those names are scored, and each scored sample gives two evaluations only once the window holds 20 values (one, from `ewma`, from 11 values).

## Grafana

Grafana is `grafana/grafana:13.2.3` on http://localhost:3000. A fresh `grafana_data` volume accepts the default `admin` / `admin`.

Dashboards are provisioned from files: `docker/grafana/provisioning/dashboards/dashboards.yml` names the provider `activity-metrics` with `allowUiUpdates: true`, and Compose mounts `docker/grafana/dashboards` read-only. A save in the UI goes to the `grafana_data` volume and never changes the JSON. To change a dashboard for good, edit its JSON in git and run `docker compose restart grafana`.

*Metrics explorer* opens on the last 30 minutes and refreshes every 5 seconds. Most of its panels filter on the sample's `timestamp`. *Metric rows ingested /s* and *Ingest delay (live data only)* filter on `inserted_at`, the insert time. The row and part counts and the *Machine* table have no time filter. The *Machine* and *Metric* lists are built when the dashboard loads, so changing the time range does not rebuild them.

## Mutation testing

`make mutate` runs `uv run mutmut run`. `[tool.mutmut]` in `pyproject.toml` mutates `src/activity_metrics/anomaly_detector/algorithms/` and `services/`, then runs `tests/anomaly_detector/`. mutmut copies only the paths it mutates, so `also_copy` lists the rest of what those tests import: the package and detector `__init__.py` files, `anomaly_detector/models`, `shared`, and `tests`. A new import from anywhere else fails the mutant run with `ImportError` until that path is added there.

`mutmut` is in the `dev` dependency group. The app image installs with `--no-default-groups`, so run this on the host after `uv sync`. The `mutants/` directory is gitignored. `pytest` is configured with `--ignore=mutants`, so `make test` does not collect a leftover tree. `uv run mutmut results` lists what survived; `uv run mutmut show <id>` prints one mutant. The Flink process function is outside `source_paths` because it imports PyFlink, which is not installed in this environment.

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and the *Metrics explorer* dashboard (uid `ad8rn6h`) respond. Each failure prints a hint. It does not check Prometheus or the *Pipeline performance* dashboard.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

CI runs the same check on every pull request and every push to `master`, in `.github/workflows/smoke.yml`. The `smoke` job starts an agent on the runner with `uv run agent`, starts the stack with `docker compose up -d --build`, checks that the `migrate` container exited with 0, runs `make migrate` again (it must succeed with no change), and then `WAIT=300 make smoke`. On failure it prints `docker compose ps -a`, the last 200 log lines of every service, and the agent's log.

## Troubleshooting

**Collector exits with `Cannot load agents: ...`.** `collector.toml` is missing, isn't valid TOML, or `agents` isn't a non-empty list of `http://` or `https://` URLs; the message names which. Compose restarts the collector, so the line repeats in `docker compose logs collector` until the file is fixed. If the message says `collector.toml` is a directory, Docker created it for the mount because the file was missing: delete it and restore the file from git. On start the collector logs `Agents from collector.toml: [...]`.

**Scrape errors each round.** The collector logs `Failed to fetch metrics from ...` for transport and HTTP errors, and `Invalid metrics from ...` for a 200 body that fails `MetricsResponse` validation. The gap before the next round is `COLLECTOR_SCRAPE_INTERVAL_SECONDS` (default 1). An entry in `collector.toml` with a path, like `http://host:8080/v1/metrics`, still scrapes `/v1/metrics` at the root: list the base URL only.

**Kafka send failures.** When `KafkaRecordsProducer.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` and the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host, `kafka:9092` from Compose services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check the writer logs and that its group is `clickhouse-metrics-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that.

**`Skipping malformed record` in a writer log.** The record is not valid JSON or lacks a required field. A metric or evaluation also needs a valid `metric_id`, an evaluation a non-empty `machine_id`, `metric_name`, and `algorithm`, and a machine a non-empty `machine_id`, a `last_boot` with a UTC offset, and a `last_seen_at`. A machine record from before migration `000002`, with `observed_at` and no `last_seen_at`, is this case. Records published before `metric_id` existed have none; they are skipped, not retried.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing until that series' window holds `min_points` values: 11 for `ewma` and 20 for `mad`. At the default 1-second gap that is about 11 and 20 seconds; at a steady 10-second cadence, about 2 and 3.5 minutes; a 5-minute NAB series needs about 100 minutes before `mad` emits. Only `system.cpu.utilization`, `system.memory.usage`, and names that start with `nab.` are scored. A `docker compose restart jobmanager` starts empty and rereads `raw_metrics` from `earliest`, so the warm-up runs again.

**A writer flush holds more rows than `WRITER_BATCH_SIZE`.** Each poll asks for at most `WRITER_BATCH_SIZE` records, the parsed rows are added to the waiting ones, and the size check runs after that. So one flush can hold up to `2 * WRITER_BATCH_SIZE - 1` rows. A timeout flush can be smaller. A poll that parses to nothing neither inserts nor commits, so after a restart the same malformed records are logged again until a later flush commits past them.

**`WRITER_BATCH_SIZE` of 0 or less crash-loops the writer.** It is passed to `poll` as `max_records`, and kafka-python raises `ValueError: max_records must be positive`. The sink fails, `main` stops the other two, and Compose restarts the process. A `WRITER_BATCH_TIMEOUT_SECONDS` of 0 or less flushes the waiting rows on every loop.

**Pipeline performance shows DOWN, or is empty.** The State row reads Prometheus scrapes, not container status. Check http://localhost:8000/metrics, http://localhost:8001/metrics and http://localhost:9090/targets. Right after `make up`, Prometheus may be up before Flink. A process started with `make` on the host is not scraped.

**`make collector` or `make clickhouse-writer` exits because port 8000 is in use.** Both bind `METRICS_PORT` (8000). The Compose collector already holds host port 8000. Stop the other process, or run both in Compose.

**A flat `nab.` series flags every rise, with the upper band on the baseline.** Floorless EWMA (`EWMA(min_deviation=0.0)`) treats a scale of 0 as "any rise is an anomaly" and sets `upper` equal to `baseline`. On the Metrics explorer the dashed upper line sits on the average and the point is marked. A flat continuation and a drop are not anomalies. This used to raise `ZeroDivisionError` and restart the job. CPU and memory keep a positive floor, so a tiny step there stays inside the band.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 300 seconds after (`COLLECTOR_MACHINE_REFRESH_SECONDS`); check its log for `Failed to fetch machine from` or `Invalid machine from` (a body missing `last_seen_at`, or a `last_boot` without a UTC offset, fails validation), and that `clickhouse-writer` is running.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain, subject to the 24-hour TTL.
