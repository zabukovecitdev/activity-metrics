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

| Target | Command | Notes |
| --- | --- | --- |
| `client` | `uv run activityreporter` | `activityreporter.agent.main:cli` |
| `collector` | `uv run collector` | `activityreporter.collector.main:cli` |
| `metrics-writer` | `uv run metrics-writer` | `activityreporter.metrics_writer.main:main` |
| `test` | `uv run pytest` | `pythonpath = ["src"]` in `pyproject.toml` |
| `mad` | `PYTHONPATH=src KAFKA_CONNECTOR_JAR=<abs jar> flink-jobs/.venv/bin/python src/activityreporter/anomaly_detector/main.py` | Jar path is absolute; see below |

`make mad` does not create `flink-jobs/.venv`. That interpreter must be CPython 3.8–3.11. `docker/flink/Dockerfile` states that range, and `apache-flink==1.19.1` depends on `pemja==0.4.1`, which publishes wheels only for 3.8–3.11. The rest of this repo requires Python 3.14, so the Flink venv is not the `uv` environment.

```bash
python3.11 -m venv flink-jobs/.venv
flink-jobs/.venv/bin/pip install 'apache-flink==1.19.1'
make mad
```

`make mad` downloads `flink-sql-connector-kafka-3.2.0-1.19.jar` into `flink-jobs/lib/`, exports that absolute filesystem path as `KAFKA_CONNECTOR_JAR`, and sets `PYTHONPATH=src` so the job imports `activityreporter` without installing the wheel into the Flink venv. Copying only the interpreter command from the table leaves `PYTHONPATH` unset and the import fails.

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
| `KAFKA_CONNECTOR_JAR` | `<dir of main.py>/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` | empty |

`KAFKA_CONNECTOR_JAR` is a filesystem path. When it is non-empty, the job sets `pipeline.jars` to `Path(value).resolve().as_uri()`. A relative path is resolved against the process working directory. Do not prefix `file://`: `Path` treats that string as a relative path, so the URI becomes `file://<cwd>/file%3A/...` and the JVM does not load the connector.

Unset, the default is the absolute path next to `anomaly_detector/main.py` (`Path(__file__).parent / "lib" / "flink-sql-connector-kafka-3.2.0-1.19.jar"`). On the cluster that directory is wherever Flink extracts the submitted script, and the jar is not there. An empty string skips `pipeline.jars`. Compose sets it empty because the image already has the connector in `/opt/flink/lib`. Loading that jar a second time is what the submitter avoids. Local runs need the variable because the `apache-flink` wheel does not include the connector. `make mad` passes the absolute path under `flink-jobs/lib/`.

`docker/flink/Dockerfile` fetches the same jar (`3.2.0-1.19` on Flink `1.19.1`) and runs `chmod 644` on it. `ADD` from a URL otherwise leaves the file mode `600`, owned by root, and the `flink` user (uid 9999) cannot read it. The job then fails at submission with the connector classes unresolved.

The image installs PyFlink `apache-flink==1.19.1`, matching the base image, on the distro `python3` (3.8–3.11). The job calls `env.set_python_executable(sys.executable)` while the graph is built in the submitter, so taskmanagers must have PyFlink at that same path. Jobmanager, taskmanager, and the submitter share `activityreporter-flink:1.19.1`. A worker started as plain `python` on `PATH` dies with `ModuleNotFoundError: pyflink`.

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

`jobmanager` and `taskmanager` mount `./flink-jobs` at `/opt/flink/jobs`. They do not mount `src/`. Only `flink-jobs-submitter` mounts `./src` at `/opt/flink/src` (read-only) and runs `docker/flink/submit-jobs.sh`.

The script submits `/opt/flink/src/activityreporter/anomaly_detector/main.py` plus, with `nullglob`, any `/opt/flink/jobs/*.py`. An empty `./flink-jobs` does not add a literal `*.py` argument. The detector path is not a glob, so it is still submitted when that directory is empty. `flink run -d -m jobmanager:8081 -pyfs /opt/flink/src -py <job>` ships `src/` to the Python workers, which is how they import `activityreporter`. `flink run` inside the jobmanager container does not see `/opt/flink/src`.

Kafka settings are read in the submitter while the graph is built. The taskmanager does not need `KAFKA_CONNECTION_STRING`. The sink writes the JSON row and does not set a Kafka key. `anomaly_processed_metrics.print()` logs each scored row on the taskmanager.

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

## Packaging

Hatchling builds the wheel from `src/activityreporter`. The installed import name is `activityreporter` (`__init__.py` is present on the package and each process directory). Console scripts:

| Script | Entry |
| --- | --- |
| `activityreporter` | `activityreporter.agent.main:cli` |
| `collector` | `activityreporter.collector.main:cli` |
| `metrics-writer` | `activityreporter.metrics_writer.main:main` |

`docker/collector/Dockerfile` and `docker/consumer/Dockerfile` `pip install` the project and start `collector` and `metrics-writer`. The root `Dockerfile` starts `activityreporter`. The anomaly detector is not a console script. Compose submits `src/activityreporter/anomaly_detector/main.py` with `flink run`.

## Troubleshooting

**Collector logs no discovered clients.** The collector must share a multicast network with the agent. In Compose it uses the host network for that reason. Confirm the agent is on the host (or another host on the LAN) and that UDP 5353 is not blocked. Add a full URL to `COLLECTOR_ENDPOINTS` to bypass mDNS. The collector logs `Static endpoints: none` when that variable is empty.

**Scrape errors every 10 seconds.** The collector logs `Failed to scrape metrics from ...` for transport and HTTP errors, and `Malformed response from ...` for a 200 body that is missing `machine_id`, `timestamp`, or a complete metric object. A trailing slash or a path other than `/v1/metrics` only works when the agent's TXT `path` matches the route you serve.

**Kafka send failures.** `KafkaConnector.send` logs `Failed to send metrics to Kafka` and re-raises. The collector swallows `KafkaError` after that log so the scrape loop continues. Check `KAFKA_CONNECTION_STRING`: `localhost:9094` from the host and from the host-network collector, `kafka:9092` from bridge-network services.

**Writer is idle while raw samples are flowing.** It reads `processed_metrics`, which is filled by the Flink job. Raw samples on `raw_metrics` do not reach TimescaleDB. Confirm the job is running in the Web UI and that the writer group is `timescale-writer`.

**Writer reprocesses the same batch.** Offsets commit only after `insert_batch` returns. A TimescaleDB error is logged as `Failed to write metrics batch to TimescaleDB (attempt n/5)` and retried. After five failures the exception propagates and the process exits; Compose restarts it (`restart: unless-stopped`) and the uncommitted batch is read again. Duplicate rows are dropped by the primary key.

**`is_anomaly` stays false.** The operator needs 20 samples of that `(machine_id, name)` in the last hour, the series must be a gauge, and the name must not be one of `system.battery.charging`, `system.battery.utilization`, or `system.memory.limit`. Battery series are omitted entirely when the agent has no power supply.

**Flink job cannot find the Kafka connector.** On a local run, set `KAFKA_CONNECTOR_JAR` to the filesystem path of the downloaded jar (`make mad` does this). A `file://` prefix is not a path the job can resolve. In the image, confirm `/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar` is mode `644`. The submitter must leave `KAFKA_CONNECTOR_JAR` empty so the image copy is the only one on the classpath. Unset is not the same as empty: the code default looks for `lib/` next to the submitted script.

**Flink job dies once a series reaches 20 samples.** The task fails in PyFlink's boolean coder (`chr()` rejects the value) the first time `MAD.is_anomaly` runs. That function must return a builtin `bool`. Comparisons on numpy scalars are `numpy.bool_`. `anomaly_detector/mad.py` wraps both return sites in `bool()`. The unit tests check `result is True` and `result is False`.

**`make mad` cannot import the job.** `ModuleNotFoundError: activityreporter` means `PYTHONPATH=src` was not set. `ModuleNotFoundError: pyflink` means `flink-jobs/.venv` is missing or was created with a Python newer than 3.11.

**Flink submitter exits immediately.** Either the jobmanager was not up (`Waiting for Flink jobmanager` repeats until it is), a job file failed (`Failed to submit ...`), or a job is already running and submission was skipped. A missing `./src` mount still attempts `/opt/flink/src/activityreporter/anomaly_detector/main.py` and fails that submission. An empty `./flink-jobs` does not, by itself, skip the detector.

**`make run` looks like it wiped the database.** It does not remove named volumes. It does recreate Kafka. TimescaleDB rows in `metrics` remain, subject to the 24-hour retention policy.
