# ActivityReporter

```
agent ──HTTP──▶ collector ──▶ Kafka raw_metrics ──▶ anomaly_detector (Flink) ──▶ Kafka processed_metrics ──▶ metrics_writer ──▶ TimescaleDB
```

## Project layout

One package, `src/activityreporter/`, split by application. Every application has the same shape:

| File            | Layer        | Holds                                                         |
|-----------------|--------------|---------------------------------------------------------------|
| `main.py`       | entrypoint   | wiring and process start (`cli()`), nothing else              |
| `api.py`        | entrypoint   | HTTP routes (agent only)                                      |
| `service.py`    | service      | the application's logic                                       |
| `repository.py` | data access  | every read/write of data, one `<Source><Data>Repository` each |
| `discovery.py`  | infra        | mDNS advertising / browsing                                   |
| `models.py`     | domain       | types used only by this application                           |

```
src/activityreporter/
  shared/            code used by more than one application: Metric, mDNS constants, ClickHouse client
  agent/             runs on every machine, serves /v1/metrics
  collector/         scrapes agents, publishes to raw_metrics
  anomaly_detector/  PyFlink job, raw_metrics → processed_metrics with is_anomaly
  metrics_writer/    processed_metrics → TimescaleDB
tests/               mirrors src/activityreporter/
```

## Running

| Command                 | Does                                                                 |
|-------------------------|----------------------------------------------------------------------|
| `make up`               | (re)starts the whole stack in Docker; Kafka is reset, TimescaleDB is kept |
| `make down`             | stops the stack                                                      |
| `make agent`            | runs the agent locally                                               |
| `make collector`        | runs the collector locally                                           |
| `make metrics-writer`   | runs the metrics writer locally                                      |
| `make anomaly-detector` | runs the Flink job locally (creates `flink/.venv` with Python 3.11)  |
| `make test`             | runs the tests                                                       |

In Docker, `anomaly-detector-submitter` submits the Flink job once the cluster is up and skips it if a job
is already running. To resubmit after a change, cancel the job in the Flink UI (http://localhost:8081) and run
`docker compose up anomaly-detector-submitter`.

Environment variables, Compose details, and troubleshooting are in [docs/operations.md](docs/operations.md).

## HTTP API

Interactive docs: `GET /` redirects to `/docs`.

| Method | Path | Body |
| --- | --- | --- |
| `GET` | `/v1/health` | `{"status": "ok"}` |
| `GET` | `/v1/machine` | `Machine` from `agent/models.py` |
| `GET` | `/v1/metrics` | One snapshot of current gauges |

`/v1/machine` is `read_machine()` in `agent/repository.py`:

| Field | Value |
| --- | --- |
| `machine_id` | `machineid.id()` |
| `hostname` | `platform.node()` |
| `os` | `platform.system()` lowercased |
| `os_version` | `platform.release()` |
| `architecture` | `platform.machine()` |
| `cores` | Logical CPU count from `psutil.cpu_count()`. `null` when the count cannot be determined. |
| `total_disk_memory` | `psutil.disk_usage(os.path.abspath(os.sep)).total`, the total size in bytes of that filesystem. |
| `total_memory` | `psutil.virtual_memory().total` |
| `uptime` | Seconds since boot (`time.time() - psutil.boot_time()`) |
| `last_boot` | UTC ISO-8601 of `psutil.boot_time()` |

`/v1/metrics` response:

```json
{
  "machine_id": "machine-id",
  "timestamp": "2026-09-28T10:16:00+00:00",
  "metrics": [
    {
      "name": "system.cpu.utilization",
      "type": "gauge",
      "unit": "%",
      "value": 12.5,
      "attributes": null
    }
  ]
}
```

`timestamp` is the sample time from `service.collect_metrics()` (`time.time()`), formatted as UTC ISO-8601.

| Name | Unit | When present |
| --- | --- | --- |
| `system.cpu.utilization` | `%` | Always. `psutil.cpu_percent(interval=0.1)`, so the handler blocks about 100 ms. |
| `system.memory.usage` | `By` | Always. Bytes used. |
| `system.memory.limit` | `By` | Always. Bytes total. |
| `system.battery.utilization` | `%` | When `read_battery()` returns a battery whose `percent` is not `None`. |
| `system.battery.charging` | `1` | When that battery's `power_plugged` is not `None`. `1.0` if plugged in, else `0.0`. |

The two battery checks are independent, so one series can appear without the other. `sensors_battery()` raises `FileNotFoundError` when `/sys/class/power_supply` is missing (typical in a container). `read_battery()` turns that into `None`, and both series are omitted.

`attributes` is reserved for extra series dimensions. Current samples leave it unset.

## Discovery

Agents advertise `_activityrep._tcp.local.` The service type is shorter than `_activityreporter` because RFC 6335 limits service names to 15 bytes.

The instance name is `{hostname label}-{first 12 characters of the machine id with dashes removed}` on that type. The hostname label is the first DNS label, truncated to 40 characters. The Zeroconf server name is separate: `activityreporter-{those same 12 characters}.local.`. Registration uses `allow_name_change=True`, so a colliding name is renamed and the process keeps running.

The agent listens on `0.0.0.0:8080` (`PORT` in `agent/main.py`). The port is fixed. The address it publishes is the local IPv4 from a UDP connect to `10.255.255.255:1` (`lan_ip` in `agent/discovery.py`). If that connect raises `OSError`, the published address is `127.0.0.1`.

TXT properties:

| Key | Value |
| --- | --- |
| `path` | `/v1/metrics` |
| `machine_id` | `machineid.id()` |
| `hostname` | `platform.node()` |

The collector resolves IPv4 only. The scrape URL is `http://{address}:{port}{path}`, and `path` falls back to `/v1/metrics` when the TXT record omits it. Resolve timeout is 3 seconds. Address or port changes (for example after DHCP) update the URL. A removal cancels an in-flight resolve so a departing agent is not re-added.

`COLLECTOR_ENDPOINTS` is a comma-separated list of full metrics URLs merged with the mDNS set on every scrape. Use it for agents that multicast cannot reach.

## Collector contract

Every 10 seconds the collector GETs each current endpoint. Connect timeout and read timeout are 2 seconds. The HTTP client allows 200 connections.

A non-2xx response, a transport error, or a payload that fails parsing is logged and skipped. One bad agent does not cancel the other scrapes in that round.

Required JSON fields: `machine_id` (string), `timestamp` (ISO-8601), `metrics` (array of objects with `name`, `type`, `unit`, `value`). `attributes` is optional. The shared timestamp is converted with `datetime.fromisoformat(...).timestamp()` and copied onto every `Metric`. `value` is cast with `float()`.

`machine_id` is the Kafka record key (`str.encode` in the producer key serializer). A missing field, a bad timestamp, or a `value` that `float()` rejects is logged as `Malformed response from ...` and that endpoint is skipped. A `machine_id` that is present but not a string passes `parse_metrics` and then raises from the serializer. `_scrape` catches `KafkaError` around publish, so this other exception leaves `Collector.run` and the process exits. Compose restarts it (`restart: unless-stopped`).

Each `Metric` is published to Kafka as JSON (`dataclasses.asdict`), keyed by `machine_id`. Producer settings: `acks=all`, `retries=3`, send timeout 10 seconds. The producer is flushed and closed on the way out of `main`, including `SIGTERM` (installed as the default interrupt handler).

Kafka record shape:

```json
{
  "timestamp": 1759054560.0,
  "name": "system.cpu.utilization",
  "type": "gauge",
  "unit": "%",
  "value": 12.5,
  "machine_id": "machine-id",
  "attributes": null
}
```

`timestamp` is epoch seconds, not an ISO-8601 string.

## Anomaly detection

`anomaly_detector/main.py` is the PyFlink job `Anomaly Detection`.

- Source topic: `KAFKA_RAW_METRICS_TOPIC` (default `raw_metrics`), consumer group `flink-metrics-aggregator`, starting offset `earliest`.
- Sink topic: `KAFKA_PROCESSED_METRICS_TOPIC` (default `processed_metrics`).
- Event time is `timestamp * 1000` milliseconds. Out-of-orderness bound is 5 seconds. Kafka record timestamps are not used.
- Parallelism is 2. The stream is keyed by `(machine_id, name)` so each series keeps its own state. A single-partition source leaves one Kafka source subtask idle; the split applies after `key_by`.
- State is a list of `(event_time, value)` pairs for that key. Each scored event drops entries older than one hour before its own timestamp (`WINDOW_MS` in `anomaly_detector/service.py`), then appends itself. The list stays in arrival order until the next scored event for that key. `MAD` scores the appended value (the last element) against every value still stored, including that point.

Scoring rules in `AnomalyDetector.process_element` (`anomaly_detector/service.py`):

- `type` other than `gauge`, and the names `system.battery.charging`, `system.battery.utilization`, and `system.memory.limit`, are passed through with `is_anomaly=false` and no state. Those series are flags or values that barely move, so a modified z-score flags ordinary steps.
- Fewer than 20 values in the hour window sets `is_anomaly=false`.
- Otherwise `MAD.is_anomaly` scores the appended value against the values still in the list.

`MAD` (`anomaly_detector/mad.py`):

- Needs at least 2 values. `is_anomaly` raises `InsufficientDataError` from `anomaly_detector/errors.py` below that count. The Flink operator does not call `MAD` below 20 values. The returned flag is a Python `bool`; a `numpy.bool_` cannot be encoded by PyFlink's boolean coder.
- Modified z-score uses scale `1.4826` and threshold `3.5`.
- When the median absolute deviation is 0, the last value is an anomaly when it differs from the median.

The row schema matches `ProcessedMetric` (`Metric` fields plus `is_anomaly`). The collector does not send `is_anomaly`; the JSON row deserializer leaves it at the boolean default so the operator can set it. `PROCESSED_METRIC_FIELD_TYPES` in `anomaly_detector/repository.py` must name the same fields as `ProcessedMetric` or the job raises `RuntimeError` at import.

`kafka_processed_metrics_sink` sets the topic and the JSON value. Records on `processed_metrics` carry a null key. `main` also calls `processed_metrics.print()`, and the taskmanager logs every scored row.

Job submission, the Kafka connector jar, and consumer-group rules are in [docs/operations.md](docs/operations.md).

## Metrics writer

`KafkaProcessedMetricsRepository` (`metrics_writer/repository.py`) reads `KAFKA_PROCESSED_METRICS_TOPIC` (default `processed_metrics`) in group `KAFKA_CONSUMER_GROUP_ID` (default `timescale-writer`). Auto-commit is off. Offset reset is `earliest`.

Records are decoded as JSON into `ProcessedMetric`. A body that raises `TypeError` or `ValueError` (invalid JSON, a JSON array, or an object that does not match the dataclass) is logged as `Skipping malformed record {topic}[{partition}]@{offset}` and left out of the batch. A batch flushes when it reaches `METRICS_WRITER_BATCH_SIZE` (default 100) or when `METRICS_WRITER_BATCH_TIMEOUT_SECONDS` (default 5) has passed since the previous flush. `poll` waits up to 1 second (`POLL_TIMEOUT_MS`) and returns at most `batch_size` records, and the size and timeout checks run after that poll returns. An empty poll does not write or commit.

`SIGTERM` calls `MetricsWriter.stop()`. The loop finishes the current poll, then flushes a partial batch. `SIGINT` raises `KeyboardInterrupt` from `run()` before that flush, and `cli` catches it. Auto-commit is off, and closing the consumer does not commit when auto-commit is off, so the open batch stays uncommitted and the next process reads it again. A successful flush commits the consumer position, including records skipped since the previous commit. Stopping after a poll that contained only malformed records leaves those offsets uncommitted as well.

`TimescaleMetricsRepository.insert_batch` writes the TimescaleDB `metrics` table, then `MetricsWriter` commits offsets. A failed insert leaves the offsets uncommitted so the batch is replayed. The insert is idempotent:

```sql
ON CONFLICT (machine_id, name, attributes, "timestamp") DO NOTHING
```

`attributes` is part of the primary key, so `None` is stored as `{}`. `timestamp` is epoch seconds passed through `to_timestamp`.

Connection errors retry up to 5 times. Backoff is `backoff_base_seconds * 2^(attempt-1)` with a 1 second base (1s, 2s, 4s, 8s). A closed connection is reopened. A live connection is rolled back and reused.

## Database

Flyway runs `db/migrations` against TimescaleDB (`activityreporter` database) from the `migrate` Compose service.

| Migration | Effect |
| --- | --- |
| V1 | `raw_metrics` hypertable, wide CPU/memory columns. |
| V2 | 24-hour retention on `raw_metrics`. |
| V3 | `raw_metrics.is_anomaly` (default false). |
| V4 | Nullable `battery_charging` and `battery_percentage` on `raw_metrics`. |
| V5 | `metrics` hypertable in long form, 24-hour retention. This is the table the writer inserts into. |

V5 leaves `raw_metrics` in place. Current collector and writer code do not insert into it. Query `metrics` for samples produced by this pipeline.

`metrics` columns: `machine_id`, `name`, `timestamp`, `type`, `unit`, `value`, `attributes` (JSONB, default `{}`), `is_anomaly`, `ingested_at`. Primary key `(machine_id, name, attributes, timestamp)`.

## Tests

Tests mirror the package directories (`tests/agent`, `tests/collector`, `tests/anomaly_detector`, `tests/metrics_writer`) and import `activityreporter.*`. `make test` runs `uv run pytest`.
