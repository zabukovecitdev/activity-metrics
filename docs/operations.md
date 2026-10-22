# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse writers, and migrations. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volume `clickhouse_data` is kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place.

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
| `agent` | `uv run agent` | `activityreporter.agent.main:cli` |
| `collector` | `uv run collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `uv run clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |
| `test` | `uv run pytest` | |
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

`CLICKHOUSE_PORT` is the HTTP port. From the host, ClickHouse is `localhost:8123` (HTTP) and `localhost:9000` (native) with the same database, user, and password. Inside Compose the writer waits until the `migrate` container exits successfully.

The topic names are set once, in the `x-topics` block of `docker-compose.yml`, and merged into every service that produces or consumes them. Services built from the root `Dockerfile` share the `x-app` block (`build`, `image`, `restart`).

## Migrations

The `migrate` service runs `migrate/migrate` with `up` on every `make up`, after the ClickHouse healthcheck passes. It connects over the native protocol (`clickhouse:9000`) with `x-multi-statement=true` (several statements per file) and `x-migrations-table-engine=MergeTree` (otherwise `schema_migrations` is a `TinyLog`). The URL lives once, in the service's `DATABASE_URL`; `docker compose run --rm migrate <args>` passes any CLI arguments through, which is what `make migrate`, `make migrate-down`, and `make migrate-new name=<name>` do.

Writing a migration:

- One DDL change per file, with `IF [NOT] EXISTS`, and a `.down.sql` that undoes it.
- ClickHouse DDL is not transactional. A file that fails halfway leaves some statements applied.
- Test `make migrate-down && make migrate` locally before committing.
- `x-multi-statement` splits on every `;`, including one inside a `--` comment, so keep semicolons out of comments.

The tree has one migration, `000001_create_tables`. It creates `metrics`, `evaluations`, and `machines`. `metrics` and `evaluations` use `PARTITION BY toYYYYMMDD(timestamp)`, a 24-hour TTL, and `ttl_only_drop_parts = 1` (a row can remain about 48 hours, until that whole day is past the TTL). `machines` has no TTL. The statements are `CREATE TABLE IF NOT EXISTS`. ClickHouse has no way to change `PARTITION BY` on a table that already exists, so this file leaves an older table as it is. `000001_create_tables.down.sql` drops all three tables. `make migrate-down` runs that only when the database is at version 1. On version 6 it fails the check below, before any `DROP`.

**`error: no migration found for version 6: read down for version 6 .: file does not exist`.** The `clickhouse_data` volume was migrated by the removed files `000002` through `000006` (hourly partitions). `make up` keeps named volumes. golang-migrate v4.20.1 (`migrate/migrate` in the Compose image) checks that a file for the version stored in `schema_migrations` still exists before it looks for a newer one. Version 6 has neither an up nor a down file, so the `migrate` container prints that line on stderr and exits 1. The `.` is the file source's own path, the directory mounted at `/migrations`. `clickhouse-writer` stays down: it waits on `service_completed_successfully`. The same line names whatever version is recorded when that version's files are gone (2 through 6).

A volume recorded at version 1, from the old hourly `metrics`-only `000001`, prints `no change` and exits 0. Migrate matches the version number and does not compare the file body, so the new `000001_create_tables` counts as already applied. `CREATE TABLE IF NOT EXISTS` leaves the hourly table in place. `evaluations` and `machines` are absent when that volume never ran the later files; the writer starts, and its inserts fail.

`force` rewrites the version in `schema_migrations` and runs no DDL. After `force 1`, the next `up` is `no change` and the hourly tables remain. Replace the volume:

```bash
docker compose down
docker volume rm "$(docker volume ls -q \
  --filter label=com.docker.compose.project.working_dir="$PWD" \
  --filter label=com.docker.compose.volume=clickhouse_data)"
make up
```

That removes `clickhouse_data` only. `flink_checkpoints`, `grafana_data`, and `prometheus_data` stay. The old tables still drop each hourly part once every row in that hour is 24 hours old, so the volume is a short-lived copy.

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
- **State:** checkpointing is off, so a restarted job starts with empty windows and reads `raw_metrics` from `earliest` again. The evaluations it writes again have the same key and collapse in `ReplacingMergeTree`.

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

Console scripts, from `[project.scripts]` in `pyproject.toml`:

| Script | Entry |
| --- | --- |
| `agent` | `activityreporter.agent.main:cli` |
| `collector` | `activityreporter.collector.main:cli` |
| `clickhouse-writer` | `activityreporter.clickhouse_writer.main:cli` |

The root `Dockerfile` builds one image, `activityreporter:latest`, for the Compose `agent` (`dev` profile), `collector`, and `clickhouse-writer` services. Each service's `command` is the script name. The anomaly detector runs in the Flink image.

The image copies `uv` 0.11.28 from `ghcr.io/astral-sh/uv:0.11.28` and installs with the image's Python 3.14:

```dockerfile
UV_PYTHON_DOWNLOADS=never uv sync --frozen --no-default-groups --no-editable --no-cache
```

`--frozen` installs the versions pinned in `uv.lock`. A `pyproject.toml` change without a matching lock fails the image build. `--no-default-groups` skips `dev` and `experiments` (`tool.uv.default-groups`). `--no-editable` installs this project into `/app/.venv`. `UV_PYTHON_DOWNLOADS=never` uses the interpreter in the image. `ENV PATH=/app/.venv/bin:$PATH` is how `command: collector` finds the script.

Commit an updated `uv.lock` in the same change as `pyproject.toml`. Local `uv sync` and `uv run`, which `make venv`, `make test`, and `make agent` call, install the default groups, including `experiments`.

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and the dashboard respond. Each failure prints a hint.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

## Troubleshooting

**Collector exits with `Cannot load agents: ...`.** `collector.toml` is missing, isn't valid TOML, or `agents` isn't a non-empty list of `http://` or `https://` URLs; the message names which. Compose restarts the collector, so the line repeats in `docker compose logs collector` until the file is fixed. If the message says `collector.toml` is a directory, Docker created it for the mount because the file was missing: delete it and restore the file from git. On start the collector logs `Agents from collector.toml: [...]`.

**Scrape errors every 10 seconds.** The collector logs `Failed to scrape metrics from ...` for transport and HTTP errors, and `Malformed response from ...` for a 200 body that is missing `machine_id`, `timestamp`, or a complete metric object. An entry in `collector.toml` with a path, like `http://host:8080/v1/metrics`, still scrapes `/v1/metrics` at the root: list the base URL only.

**Kafka send failures.** When `KafkaRawMetricsRepository.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` and the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host, `kafka:9092` from Compose services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check the writer logs and that its group is `clickhouse-metrics-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that.

**`Skipping malformed record` in a writer log.** The record is not valid JSON or lacks a required field. A metric or evaluation also needs a valid `metric_id`, an evaluation a non-empty `machine_id`, `metric_name`, and `algorithm`, and a machine a non-empty `machine_id` and a `last_boot` with a UTC offset. Records published before `metric_id` existed have none; they are skipped, not retried.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing for a series until the last hour holds `min_points` samples of that `(machine_id, name)`: 11 for `ewma` and 20 for `mad`, about 2 and 3.5 minutes at the 10-second scrape interval. Only `system.cpu.utilization` and `system.memory.usage` are scored. A job started before this change still publishes to `anomalies`; `make up` recreates the cluster with the new job.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 5 minutes after; check its log for `Failed to fetch machine info` or `Malformed machine info` (an agent older than this change has no `observed_at`), and that `clickhouse-writer` is running.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain until their daily part expires (about 48 hours; see [Migrations](#migrations)). A volume left by the old hourly migrations is kept too, and `migrate` exits 1 with `no migration found for version 6`; that section has the volume reset.
