# Activity Reporter

Host agents sample CPU, memory, and battery and serve them over HTTP. A collector scrapes those agents, a PyFlink job scores gauge anomalies, and a writer stores the scored samples in TimescaleDB.

Console scripts and in-module imports still use the package names from before the move under `src/activityreporter/`. [docs/operations.md](docs/operations.md) lists those references and how to run the stack.

Python 3.14+. Dependencies and console scripts are declared in `pyproject.toml`. Local commands go through `uv` (`Makefile`).

## Runtime

```
agent :8080
  GET /v1/metrics          mDNS  _activityrep._tcp.local.
        |                         |
        |    scrape every 10s    |
        v                         v
     collector  --Kafka raw_metrics-->  anomaly detector (PyFlink, MAD)
                                              |
                                              v
                                    Kafka processed_metrics
                                              |
                                              v
                                    metrics writer --> TimescaleDB metrics
```

ClickHouse is started by `docker-compose.yml` and `shared/clickhouse.py` can open an async client. No pipeline service queries or inserts into it.

## Package layout

The tree under `src/activityreporter/` is split by process, with a service module for the workflow and a repository module for I/O.

| Path | Role |
| --- | --- |
| `agent/main.py` | Uvicorn entry. Binds `0.0.0.0:8080`. Advertises the process over mDNS for the process lifetime. |
| `agent/api.py` | FastAPI routes under `/v1`. |
| `agent/service.py` | Samples host gauges (`MetricFactory`). |
| `agent/models.py` | `Machine` inventory (`MachineFactory`). |
| `agent/discovery.py` | mDNS advertiser. |
| `collector/main.py` | Scrape loop entry. Flushes Kafka on `SIGTERM`. |
| `collector/service.py` | HTTP scrape and response parsing. |
| `collector/repository.py` | Kafka producer for raw samples. |
| `collector/discovery.py` | mDNS browser. Builds scrape URLs. |
| `anomaly_detector/main.py` | PyFlink job `Anomaly Detection`. |
| `anomaly_detector/mad.py` | Median absolute deviation test. |
| `metrics_writer/main.py` | Consumer entry. Stops the loop on `SIGTERM`. |
| `metrics_writer/service.py` | Polls Kafka and flushes batches. |
| `metrics_writer/repository.py` | Inserts batches into TimescaleDB. |
| `shared/metrics.py` | `Metric` and `ProcessedMetric`. |
| `shared/discovery.py` | mDNS service type and metrics path. |
| `shared/clickhouse.py` | Async ClickHouse client. |

Imports, console scripts, the Hatch package list, and tests still use the module names from before this move (`client`, `collector`, `connectors`, `consumers`, `core`). The map and the commands that depend on it are in [docs/operations.md](docs/operations.md).

## HTTP API

Interactive docs: `GET /` redirects to `/docs`.

| Method | Path | Body |
| --- | --- | --- |
| `GET` | `/v1/health` | `{"status": "ok"}` |
| `GET` | `/v1/machine` | `Machine` from `agent/models.py` |
| `GET` | `/v1/metrics` | One snapshot of current gauges |

`/v1/machine` fields: `machine_id`, `hostname`, `os` (lowercased `platform.system()`), `os_version`, `architecture`, `cores`, `total_disk_memory`, `total_memory`, `uptime` (seconds since boot), `last_boot` (UTC ISO-8601). `machine_id` comes from `machineid.id()`.

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

`timestamp` is UTC `datetime.now().isoformat()`. Each observation is produced by `MetricFactory.create_metrics()`.

| Name | Unit | When present |
| --- | --- | --- |
| `system.cpu.utilization` | `%` | Always. `psutil.cpu_percent(interval=0.1)`, so the handler blocks about 100 ms. |
| `system.memory.usage` | `By` | Always. Bytes used. |
| `system.memory.limit` | `By` | Always. Bytes total. |
| `system.battery.utilization` | `%` | Only when `psutil.sensors_battery()` returns a battery. |
| `system.battery.charging` | `1` | Only when a battery is present. `1.0` if `power_plugged`, else `0.0`. |

`sensors_battery()` raises `FileNotFoundError` when `/sys/class/power_supply` is missing (typical in a container). That error is treated as "no battery", and the two battery series are omitted.

`attributes` is reserved for extra series dimensions. Current samples leave it unset.

## Discovery

Agents advertise `_activityrep._tcp.local.` The service type is shorter than `_activityreporter` because RFC 6335 limits service names to 15 bytes.

The instance name is `{hostname label}-{first 12 characters of the machine id with dashes removed}` on that type. The hostname label is the first DNS label, truncated to 40 characters. TXT properties:

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
- State is the last hour of `(event_time, value)` pairs for that key.

Scoring rules in `AnomalyDetector.process_element`:

- `type` other than `gauge`, and the names `system.battery.charging`, `system.battery.utilization`, and `system.memory.limit`, are passed through with `is_anomaly=false` and no state. Those series are flags or values that barely move, so a modified z-score flags ordinary steps.
- Fewer than 20 values in the hour window sets `is_anomaly=false`.
- Otherwise `MAD.is_anomaly` scores the latest value against the window.

`MAD` (`anomaly_detector/mad.py`):

- Needs at least 2 values. `is_anomaly` imports `InsufficientDataError` from `core.errors.insufficient_data_error` and raises it below that count. That module was removed in the layout move, and the Flink operator does not call `MAD` below 20 values.
- Modified z-score uses scale `1.4826` and threshold `3.5`.
- When the median absolute deviation is 0, the last value is an anomaly when it differs from the median.

The row schema matches `ProcessedMetric` (`Metric` fields plus `is_anomaly`). The collector does not send `is_anomaly`; the JSON row deserializer leaves it at the boolean default so the operator can set it. `METRIC_FIELD_TYPES` must name the same fields as `ProcessedMetric` or the job raises `RuntimeError` at import.

Job submission, the Kafka connector jar, and consumer-group rules are in [docs/operations.md](docs/operations.md).

## Metrics writer

`metrics_writer/service.py` reads `KAFKA_PROCESSED_METRICS_TOPIC` (default `processed_metrics`) in group `KAFKA_CONSUMER_GROUP_ID` (default `timescale-writer`). Auto-commit is off. Offset reset is `earliest`.

Records are decoded as JSON into `ProcessedMetric`. A batch flushes when it reaches `CONSUMER_BATCH_SIZE` (default 100) or when `CONSUMER_BATCH_TIMEOUT_SECONDS` (default 5) has passed since the previous flush. `SIGTERM` stops the loop and flushes a partial batch. An empty poll does not write or commit.

`insert_batch` writes the TimescaleDB `metrics` table, then the consumer commits offsets. A failed insert leaves the offsets uncommitted so the batch is replayed. The insert is idempotent:

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

Tests mirror the package directories (`tests/agent`, `tests/collector`, `tests/anomaly_detector`, `tests/metrics_writer`) and import the pre-move module names. `make test` runs `uv run pytest`.
