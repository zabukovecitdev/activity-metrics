# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, ClickHouse, Grafana, and migrations. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make up` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. The named volumes `clickhouse_data` and `grafana_data` are kept. Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place. Neither command passes `-v`.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |
| `3000` | Grafana. |

The `agent` Compose service is in the `dev` profile, so `make up` doesn't start it; `docker compose --profile dev up -d` does. It does not publish port `8080`. The collector uses `network_mode: host` because mDNS multicast does not cross the Docker bridge, and on the host network the DNS name `kafka` does not resolve. Its `KAFKA_CONNECTION_STRING` is therefore `localhost:9094`.

An agent run on the host (`make agent`) advertises on the LAN, which that collector can discover. The Compose agent is on the bridge network with no published port, and `COLLECTOR_ENDPOINTS` defaults to empty, so this collector has no URL for it unless you set `COLLECTOR_ENDPOINTS` to an address the host network can reach.

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
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `localhost:9094` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `COLLECTOR_ENDPOINTS` | empty | empty |

`COLLECTOR_ENDPOINTS` is split on commas. Empty items are dropped. Example: `http://192.168.1.20:8080/v1/metrics,http://192.168.1.21:8080/v1/metrics`.

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

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` |
| `KAFKA_MACHINES_TOPIC` | `machines` | `machines` |
| `KAFKA_CONSUMER_GROUP_ID` | `clickhouse-writer` | `clickhouse-writer` |
| `WRITER_BATCH_SIZE` | `100` | unset |
| `WRITER_BATCH_TIMEOUT_SECONDS` | `5.0` | unset |
| `CLICKHOUSE_HOST` | `localhost` | `clickhouse` |
| `CLICKHOUSE_PORT` | `8123` | `8123` |
| `CLICKHOUSE_DB` | `metrics` | `metrics` |
| `CLICKHOUSE_USER` | `user` | `user` |
| `CLICKHOUSE_PASSWORD` | `password` | `password` |

`CLICKHOUSE_PORT` is the HTTP port. From the host, ClickHouse is `localhost:8123` (HTTP) and `localhost:9000` (native) with the same database, user, and password. Inside Compose the writer waits until the `migrate` container exits successfully.

The topic names are set once, in the `x-topics` block of `docker-compose.yml`, and merged into every service that produces or consumes them. Services built from the root `Dockerfile` share the `x-app` block (`build`, `image`, `restart`).

## ClickHouse server

The `clickhouse` service runs `clickhouse/clickhouse-server:26.3` (the 26.3 LTS line). `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` are `metrics`, `user`, and `password`. The `clickhouse_data` volume is mounted at `/var/lib/clickhouse`. The healthcheck is `SELECT 1` with that user and password and no database name, so the container can be healthy while `metrics` does not exist.

The 26.3 entrypoint creates `CLICKHOUSE_DB` only when `/var/lib/clickhouse/data` is absent. It starts a server bound to localhost, runs `CREATE DATABASE IF NOT EXISTS metrics`, then replaces that process with the normal server. The next start finds `data` and logs `ClickHouse Database directory appears to contain a database; Skipping initialization`. Changing `CLICKHOUSE_DB` after that does not create another database.

`clickhouse/clickhouse-server:24.1` inverts that test (the `v24.1.8.22-stable` entrypoint). On a volume that has no `data` directory it logs the same skipping line and never applies `CLICKHOUSE_DB`. The server still creates `data` on that first start. `migrate` then connects with `database=metrics` and fails with `Database metrics does not exist`. A second start of 24.1 would have taken the other branch and created the database. Pointing 26.3 at a volume 24.1 has already started does not: `data` is already there, so initialization is skipped. `24.8` uses the same direction as `26.3`.

`make down` and `make up` keep the volume. To add the missing database without dropping it:

```bash
docker compose exec clickhouse clickhouse-client -u user --password password -q "CREATE DATABASE IF NOT EXISTS metrics"
make migrate
```

To let 26.3 initialize a new volume, remove `clickhouse_data` (Compose names it `<project>_clickhouse_data`; `docker volume ls` shows it) and run `make up`. `docker compose down -v` removes every volume in this file, including `grafana_data`.

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
- **State:** checkpointing is off, so a restarted job starts with empty windows and reads `raw_metrics` from `earliest` again. The evaluations it writes again have the same key and collapse in `ReplacingMergeTree`.

The taskmanager still runs as its own service and registers with the jobmanager at `jobmanager:6123`.

Every reader has its own consumer group. They all start at `earliest`, and each must see every record on its topic; two readers sharing a group would split the partitions between them.

| Process | Group id | Topic it reads |
| --- | --- | --- |
| ClickHouse writer | `clickhouse-writer` | `raw_metrics`, `evaluations`, `machines` |
| Anomaly detector | `anomaly-detector` | `raw_metrics` |

The writer does not depend on the Flink job: `metrics` and `machines` fill even while the job is down.

The writer's group replaced the three per-table groups `clickhouse-metrics-writer`, `clickhouse-evaluations-writer`, and `clickhouse-machines-writer`. A new group starts at `earliest`, so on its first run it rewrites what Kafka still holds; `ReplacingMergeTree` collapses those rows.

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

## Smoke test

`make smoke` runs `scripts/smoke-test.sh` against the running stack. It checks that the containers are up and `migrate` exited 0, that `raw_metrics`, `machines`, and `evaluations` have messages, that the Flink job `Anomaly Detection` is `RUNNING` (`/jobs/overview` on port 8081), that `metrics`, `machines`, and `evaluations` have rows from the last 10 minutes and every evaluation's `metric_id` is in `metrics`, and that Grafana, its ClickHouse datasource, and the dashboard respond. Each failure prints a hint.

It needs an agent the collector can reach (`make agent` on the host) and, for the evaluation checks, a few minutes of samples: `WAIT=300 make smoke` retries failed checks for up to 300 seconds. Grafana is called as `admin:admin`; set `GRAFANA_AUTH` if you changed it.

## Grafana

`grafana` is `grafana/grafana:13.2.3`, published on port `3000`. Compose does not set `GF_SECURITY_ADMIN_USER` or `GF_SECURITY_ADMIN_PASSWORD`. A fresh `grafana_data` volume uses the image default, `admin` / `admin`, which is what `scripts/smoke-test.sh` sends unless `GRAFANA_AUTH` is set. A volume that already holds a Grafana database keeps the password stored in it.

`GF_PLUGINS_PREINSTALL_SYNC=grafana-clickhouse-datasource` installs the ClickHouse plugin before Grafana starts, so the provisioned datasource's type exists. The plugin version is not pinned. Grafana waits until ClickHouse is healthy. It does not wait for `migrate`, so the UI can come up before `metrics`, `evaluations`, and `machines` exist.

| Mount | Container path | Mode |
| --- | --- | --- |
| `grafana_data` | `/var/lib/grafana` | read-write |
| `./docker/grafana/provisioning` | `/etc/grafana/provisioning` | read-only |
| `./docker/grafana/dashboards` | `/var/lib/grafana/dashboards` | read-only |

`docker/grafana/provisioning/datasources/clickhouse.yml` adds a datasource named `ClickHouse`, uid `clickhouse`, type `grafana-clickhouse-datasource`, and makes it the default. It uses the native protocol: host `clickhouse`, port `9000`, user `user`, password `password`, database `metrics`. That is not the HTTP port `8123` the writer uses. From the host, the same native port is `localhost:9000`.

`docker/grafana/provisioning/dashboards/dashboards.yml` is a file provider named `activityreporter`. It reads `/var/lib/grafana/dashboards` and sets `allowUiUpdates: true`. It does not set `updateIntervalSeconds`. The only dashboard file is `machine-usage-metrics.json`: title *Machine usage metrics*, uid `ad8rn6h` (the uid `make smoke` requests). The JSON is mounted read-only, so an edit in the UI stays in Grafana's database on `grafana_data` and is not written back to the file. `make down` keeps that volume.

Which queries follow the time picker:

| Panel | Query |
| --- | --- |
| **CPU**, **Uptime** | Latest row in `metrics` for that series. No time filter, no `FINAL`. |
| **Memory**, **Memory used** | Last hour (`now() - INTERVAL 1 HOUR`), no `FINAL`. Memory is `system.memory.usage / system.memory.limit`. |
| **Anomalies**, **CPU usage**, **Memory usage**, **Detector explorer** | `$__timeFilter(timestamp)` and `FINAL`. Anomaly points join `evaluations` to `metrics` on `metric_id`. |

A replayed batch can still show two rows for one sample until ClickHouse merges them. The stat panels can then disagree with the charts, which read `FINAL`. Panel contents are in the README.

## Troubleshooting

**Collector logs no discovered clients.** The collector must share a multicast network with the agent. In Compose it uses the host network for that reason. Confirm the agent is on the host (or another host on the LAN) and that UDP 5353 is not blocked. Add a full URL to `COLLECTOR_ENDPOINTS` to bypass mDNS. The collector logs `Static endpoints: none` when that variable is empty.

**Scrape errors every 10 seconds.** The collector logs `Failed to scrape metrics from ...` for transport and HTTP errors, and `Malformed response from ...` for a 200 body that is missing `machine_id`, `timestamp`, or a complete metric object. A trailing slash or a path other than `/v1/metrics` only works when the agent's TXT `path` matches the route you serve.

**Kafka send failures.** When `KafkaRawMetricsRepository.publish` raises `KafkaError`, the collector logs `Failed to publish metrics from ... to Kafka` and the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host and from the host-network collector, `kafka:9092` from bridge-network services.

**`metrics` stays empty while raw samples are flowing.** The writer reads `raw_metrics` directly. Check its logs and that its group is `clickhouse-writer`, not a group another process also uses.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns for every table in the flush. A ClickHouse error is logged as `Failed to write <table> batch to ClickHouse (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. The replayed rows are inserted again and collapsed by `ReplacingMergeTree` on merge; query with `FINAL` to hide them before that.

**`Skipping malformed record` in a writer log.** The record is not valid JSON or lacks a required field. A metric or evaluation also needs a valid `metric_id`, an evaluation a non-empty `machine_id`, `metric_name`, and `algorithm`, and a machine a non-empty `machine_id` and a `last_boot` with a UTC offset. Records published before `metric_id` existed have none; they are skipped, not retried.

**`evaluations` stays empty.** Confirm the Flink job runs and that `evaluations` receives records (`kafka-console-consumer.sh --topic evaluations`). A detector emits nothing for a series until the last hour holds `min_points` samples of that `(machine_id, name)`: 11 for `ewma` and 20 for `mad`, about 2 and 3.5 minutes at the 10-second scrape interval. The series must be a gauge, and the name must not be one of `system.battery.charging`, `system.battery.utilization`, `system.memory.limit`, or `system.uptime`. Battery series are omitted entirely when the agent has no power supply. A job started before this change still publishes to `anomalies`; `make up` recreates the cluster with the new job.

**Bands drop to 0 in a custom Grafana query.** A `LEFT JOIN` from `metrics` to `evaluations` fills unmatched rows with `0` unless the query ends with `SETTINGS join_use_nulls = 1`.

**The Machine drop-down shows an id instead of a hostname.** `machines` has no row for it yet. The collector fetches `/v1/machine` on its first scrape of an endpoint and every 5 minutes after; check its log for `Failed to fetch machine info` or `Malformed machine info` (an agent older than this change has no `observed_at`), and that `clickhouse-writer` is running.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The jobmanager leaves `KAFKA_CONNECTOR_JAR` unset so the image copy is the only one on the classpath.

**Flink jobmanager keeps restarting.** In Application Mode the jobmanager exits when the job can't start, for example on a Python error in `main` or an import error in `./src`. `docker compose logs jobmanager` shows the traceback. A job that starts and then fails is restarted by Flink first, and shows up under *Exceptions* in the Web UI.

**`make up` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. ClickHouse rows in `metrics` remain, subject to the 24-hour TTL. `docker compose down -v` does remove `clickhouse_data` and `grafana_data`.

**`migrate` fails with `Database metrics does not exist`.** ClickHouse can still be healthy: its healthcheck is `SELECT 1` and does not name a database. `CLICKHOUSE_DB` is applied only while `/var/lib/clickhouse/data` is absent, and only by an image whose entrypoint is not inverted (`24.8` and newer; this stack pins `26.3`). A volume that `24.1` already started has that directory and no `metrics` database, and changing the image tag does not create it. Recovery is in [ClickHouse server](#clickhouse-server).

**Grafana panels error or the datasource is red.** The service starts after ClickHouse is healthy, not after `migrate`, so the tables may not exist yet. The datasource speaks native protocol to `clickhouse:9000` (database `metrics`, user `user`, password `password`); port `8123` is the HTTP port and will not answer it. If the plugin failed to install, the datasource type `grafana-clickhouse-datasource` is missing. `GF_PLUGINS_PREINSTALL_SYNC` installs it before startup; `docker compose logs grafana` shows that step.
