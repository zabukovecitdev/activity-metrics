# Operations

Setup, configuration, and failure modes for the agent, collector, Flink job, and metrics writer. Behavior below is what the current source and Compose file do. See [README](../README.md) for the data model and API.

## Local stack

`make run` runs `docker compose down --remove-orphans` and then `docker compose up -d --build`. Named volumes are kept (`timescaledb-data`, `clickhouse_data`). Kafka has no volume, so topics and consumer offsets are recreated. `make down` stops the project and leaves those volumes in place.

| Published port | Service |
| --- | --- |
| `9094` | Kafka listener advertised as `localhost:9094` (`PLAINTEXT_HOST`). Containers on the Compose network use `kafka:9092`. |
| `5433` | TimescaleDB, mapped to container port `5432`. |
| `8123`, `9000` | ClickHouse HTTP and native protocol. |
| `8081` | Flink Web UI. |

The `activityreporter` Compose service does not publish port `8080`. The collector uses `network_mode: host` because mDNS multicast does not cross the Docker bridge, and on the host network the DNS name `kafka` does not resolve. Its `KAFKA_CONNECTION_STRING` is therefore `localhost:9094`.

An agent run on the host (`make client`) advertises on the LAN, which that collector can discover. The Compose agent is on the bridge network with no published port, and `COLLECTOR_ENDPOINTS` defaults to empty, so this collector has no URL for it unless you set `COLLECTOR_ENDPOINTS` to an address the host network can reach.

Process commands from the `Makefile`:

| Target | Command | Entry module in `pyproject.toml` |
| --- | --- | --- |
| `client` | `uv run activityreporter` | `client.main:cli` |
| `collector` | `uv run collector` | `collector.main:cli` |
| `metrics-writer` | `uv run metrics-writer` | `consumers.main:main` |
| `test` | `uv run pytest` | |
| `mad` | `flink-jobs/.venv/bin/python flink-jobs/metrics_aggregator.py` | Job file now lives at `src/activityreporter/anomaly_detector/main.py` |

`mad` also downloads `flink-sql-connector-kafka-3.2.0-1.19.jar` into `flink-jobs/lib/`. The job's default jar path is next to `anomaly_detector/main.py` (`anomaly_detector/lib/...`), not `flink-jobs/lib/`.

## Environment

Defaults are the `from_env` / `os.environ.get` fallbacks. Compose overrides are noted when they differ.

### Collector

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `localhost:9094` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `COLLECTOR_ENDPOINTS` | empty | empty |

`COLLECTOR_ENDPOINTS` is split on commas. Empty items are dropped. Example: `http://192.168.1.20:8080/v1/metrics,http://192.168.1.21:8080/v1/metrics`.

### Anomaly detector

| Variable | Default | Compose (`flink-jobs-submitter`) |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `raw_metrics` |
| `KAFKA_PROCESSED_METRICS_TOPIC` | `processed_metrics` | `processed_metrics` |
| `KAFKA_CONNECTOR_JAR` | `anomaly_detector/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` | empty |

An empty `KAFKA_CONNECTOR_JAR` skips `pipeline.jars`. The Flink image already has that connector in `/opt/flink/lib`, and loading it a second time is what the submitter avoids. Local runs need the jar on the classpath because the `apache-flink` wheel does not include it. Pass an absolute `file://` path via `KAFKA_CONNECTOR_JAR`.

`docker/flink/Dockerfile` fetches the same jar (`3.2.0-1.19` on Flink `1.19.1`) and runs `chmod 644` on it. `ADD` from a URL otherwise leaves the file mode `600`, owned by root, and the `flink` user (uid 9999) cannot read it. The job then fails at submission with the connector classes unresolved.

The image installs PyFlink `apache-flink==1.19.1`, matching the base image. The job calls `env.set_python_executable(sys.executable)` so keyed Python operators use that interpreter. A worker started as plain `python` on `PATH` dies with `ModuleNotFoundError: pyflink`.

### Metrics writer

| Variable | Default | Compose |
| --- | --- | --- |
| `KAFKA_CONNECTION_STRING` | `localhost:9094` | `kafka:9092` |
| `KAFKA_PROCESSED_METRICS_TOPIC` | `processed_metrics` | unset (default applies) |
| `KAFKA_CONSUMER_GROUP_ID` | `timescale-writer` | `timescale-writer` |
| `CONSUMER_BATCH_SIZE` | `100` | unset |
| `CONSUMER_BATCH_TIMEOUT_SECONDS` | `5.0` | unset |
| `TIMESCALE_HOST` | `localhost` | `timescaledb` |
| `TIMESCALE_PORT` | `5432` | `5432` |
| `TIMESCALE_DB` | `activityreporter` | `activityreporter` |
| `TIMESCALE_USER` | `activityreporter` | `activityreporter` |
| `TIMESCALE_PASSWORD` | `activityreporter` | `activityreporter` |

Compose sets `KAFKA_RAW_METRICS_TOPIC` on this service. The writer does not read that variable. It consumes `KAFKA_PROCESSED_METRICS_TOPIC`.

From the host, Timescale is `localhost:5433` with the same database, user, and password. Inside Compose the writer uses host `timescaledb` port `5432`, and it waits until the Flyway `migrate` container exits successfully.

### ClickHouse client

| Variable | Default | Compose server |
| --- | --- | --- |
| `CLICKHOUSE_HOST` | `localhost` | container name `clickhouse-server` |
| `CLICKHOUSE_PORT` | `8123` | `8123` |
| `CLICKHOUSE_USER` | `user` | `user` |
| `CLICKHOUSE_PASSWORD` | `password` | `password` |
| `CLICKHOUSE_DB` | `metrics` | `metrics` |

`ClickHouseConnector.from_env` sets `autogenerate_session_id=False`. One client is shared across coroutines, and a per-client session rejects concurrent queries. Running `shared/clickhouse.py` as a script prints the result of `SELECT version()`. Nothing else in the repo calls the connector.

## Flink jobs

`jobmanager` and `taskmanager` mount `./flink-jobs` at `/opt/flink/jobs`. `flink-jobs-submitter` runs `docker/flink/submit-jobs.sh`, which submits every `/opt/flink/jobs/*.py` with `-pyfs /opt/flink/src` (the repo `src/` tree) and `-d`. The job Python file is `src/activityreporter/anomaly_detector/main.py`, so the submitter does not see it in `/opt/flink/jobs`.

The submitter waits until `flink list -m jobmanager:8081` succeeds. If the cluster already has a running job, it prints `Cluster already has running jobs, skipping submission.` and exits 0. To submit again, cancel the job in the Web UI at `http://localhost:8081`, then:

```bash
docker compose up flink-jobs-submitter
```

Give the Flink source and the writer different consumer groups. Both start at `earliest` and each must see every record on its topic.

| Process | Group id | Topic it reads |
| --- | --- | --- |
| Anomaly detector | `flink-metrics-aggregator` | `raw_metrics` |
| Metrics writer | `timescale-writer` | `processed_metrics` |

`jobmanager` sets `restart-strategy.type: exponential-delay`. Without a restart strategy the job stops for good on the first error, including starting before the collector has created `raw_metrics`.

Task slots on the taskmanager: 2, matching the job parallelism.

## Packaging after the layout move

Files live under `src/activityreporter/`. These references still use the previous locations:

| Still configured as | Current file |
| --- | --- |
| `client.main:cli` | `activityreporter/agent/main.py` (`cli`) |
| `collector.main:cli` | `activityreporter/collector/main.py` (`cli`) |
| `consumers.main:main` | `activityreporter/metrics_writer/main.py` (`main`) |
| Hatch wheel packages `src/client`, `src/collector`, `src/connectors`, `src/consumers`, `src/core` | Those directories are gone. The wheel config does not list `src/activityreporter`. |
| `docker/collector/Dockerfile`, `docker/consumer/Dockerfile` | Removed. `docker-compose.yml` still builds the collector and metrics-writer from those paths. The root `Dockerfile` remains and starts `activityreporter`. |
| `flink-jobs/metrics_aggregator.py` | `src/activityreporter/anomaly_detector/main.py` |
| `from client.reporter import HttpReporter` and `from client.machine_info import Machine, MachineFactory` in `agent/api.py` | `reporter.py` was removed. `Machine` and `MachineFactory` are in `agent/models.py`. Sampling is `MetricFactory` in `agent/service.py`. |
| `from core.errors.insufficient_data_error import InsufficientDataError` in `anomaly_detector/mad.py` | That module was removed. |

Other imports inside the moved files follow the same old top-level names (`client`, `collector`, `connectors`, `consumers`, `core`). Tests patch and import those names (`client.metric_factory`, `collector.collector`, `consumers.metrics_consumer`, `connectors.timescale_connector`, `core.mad`, `core.discovery`, `core.metrics`).

There are no `__init__.py` files under `src/activityreporter/`.

## Troubleshooting

**Collector logs no discovered clients.** The collector must share a multicast network with the agent. In Compose it uses the host network for that reason. Confirm the agent is on the host (or another host on the LAN) and that UDP 5353 is not blocked. Add a full URL to `COLLECTOR_ENDPOINTS` to bypass mDNS. The collector logs `Static endpoints: none` when that variable is empty.

**Scrape errors every 10 seconds.** The collector logs `Failed to scrape metrics from ...` for transport and HTTP errors, and `Malformed response from ...` for a 200 body that is missing `machine_id`, `timestamp`, or a complete metric object. A trailing slash or a path other than `/v1/metrics` only works when the agent's TXT `path` matches the route you serve.

**Kafka send failures.** `KafkaConnector.send` logs `Failed to send metrics to Kafka` and re-raises. The collector swallows `KafkaError` after that log so the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host and from the host-network collector, `kafka:9092` from bridge-network services.

**Writer is idle while raw samples are flowing.** It reads `processed_metrics`, which is filled by the Flink job. Raw samples on `raw_metrics` do not reach TimescaleDB. Confirm the job is running in the Web UI and that the writer group is `timescale-writer`.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A TimescaleDB error is logged as `Failed to write metrics batch to TimescaleDB (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. Duplicate rows are dropped by the primary key.

**`is_anomaly` stays false.** The operator needs 20 samples of that `(machine_id, name)` in the last hour, the series must be a gauge, and the name must not be one of `system.battery.charging`, `system.battery.utilization`, or `system.memory.limit`. Battery series are omitted entirely when the agent has no power supply.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the downloaded jar. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The submitter must leave `KAFKA_CONNECTOR_JAR` empty so the image copy is the only one on the classpath.

**Flink submitter exits immediately.** Either the jobmanager was not up (`Waiting for Flink jobmanager` repeats until it is), a `*.py` under the jobs mount failed (`Failed to submit ...`), or a job is already running and submission was skipped. The jobs mount is `./flink-jobs`, which this tree no longer contains; Compose will create an empty directory, and the submitter then has nothing to submit.

**`make run` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. TimescaleDB rows in `metrics` remain, subject to the 24-hour retention policy.
