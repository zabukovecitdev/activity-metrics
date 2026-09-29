# ActivityReporter

```
agent ──HTTP──▶ collector ──▶ Kafka raw_metrics ─┬─▶ metrics-writer ─────────────────────────────────────────────▶ ClickHouse metrics
                                                 └─▶ anomaly_detector (Flink) ──▶ Kafka anomalies ──▶ anomalies-writer ──▶ ClickHouse anomalies
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
  shared/            code used by more than one application: Metric, Anomaly, mDNS constants
  agent/             runs on every machine, serves /v1/metrics
  collector/         scrapes agents, publishes to raw_metrics
  anomaly_detector/  PyFlink job, raw_metrics → anomalies (only the anomalous samples)
  clickhouse_writer/ Kafka topic → ClickHouse table: raw_metrics → metrics, anomalies → anomalies
tests/               mirrors src/activityreporter/
```

## Running

| Command                 | Does                                                                 |
|-------------------------|----------------------------------------------------------------------|
| `make up`               | (re)starts the whole stack in Docker; Kafka is reset, ClickHouse is kept |
| `make migrate`          | applies pending ClickHouse migrations                                |
| `make migrate-down`     | reverts the latest migration                                         |
| `make migrate-new name=x` | creates the next numbered `up`/`down` migration pair               |
| `make down`             | stops the stack                                                      |
| `make agent`            | runs the agent locally                                               |
| `make collector`        | runs the collector locally                                           |
| `make metrics-writer`   | runs the metrics writer locally                                      |
| `make anomalies-writer` | runs the anomalies writer locally                                    |
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

`timestamp` is the sample time from `service.collect_metrics()` (`time.time()`), formatted as UTC ISO-8601.

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

Required JSON fields: `machine_id` (string), `timestamp` (ISO-8601), `metrics` (array of objects with `name`, `type`, `unit`, `value`). `attributes` is optional. The shared timestamp is converted with `datetime.fromisoformat(...).timestamp()` and copied onto every `Metric`. `value` is cast with `float()`. Each `Metric` gets its own `metric_id`, a UUIDv7 string from `uuid.uuid7()`: the sample's identity from here on, in Kafka, in `metrics`, and in any anomaly it causes. The agent does not send one.

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
  "metric_id": "01a0e974-dc94-74df-ae87-e0e6064b18ef",
  "attributes": null
}
```

`timestamp` is epoch seconds, not an ISO-8601 string.

## Anomaly detection

`anomaly_detector/main.py` is the PyFlink job `Anomaly Detection`.

- Source topic: `KAFKA_RAW_METRICS_TOPIC` (default `raw_metrics`), consumer group `anomaly-detector`, starting offset `earliest`.
- Sink topic: `KAFKA_ANOMALIES_TOPIC` (default `anomalies`). Only anomalous samples are published; everything else produces no output. The same rows are printed on the taskmanager (`anomalies.print()`).
- Event time is `timestamp * 1000` milliseconds. Out-of-orderness bound is 5 seconds. Kafka record timestamps are not used.
- Parallelism is 2. The stream is keyed by `(machine_id, name)`. Samples that share those two fields share one window, including when `attributes` differ. `metric_attributes` is copied onto the anomaly so the row can be read on its own. A single-partition source leaves one Kafka source subtask idle; the split applies after `key_by`.
- State is the last hour of `(event_time, value)` pairs for that key. Each scored record drops pairs older than one hour before its own event time, appends itself, and leaves the list until the next scored record. Unscored names never enter the list.

Scoring rules (`anomaly_detector/detection.py`, called from `AnomalyDetector.process_element` in `service.py`):

- `type` other than `gauge`, and the names `system.battery.charging`, `system.battery.utilization`, and `system.memory.limit`, are dropped without state. Those series are flags or values that barely move, so a modified z-score flags ordinary steps.
- Fewer than 20 values in the hour window: no anomaly.
- Otherwise `MAD.score` scores the appended value (`values[-1]`, this record) against the window. That is the record being processed, including when it arrives out of timestamp order. A score of at least `MAD.THRESHOLD` is an anomaly.

`detection.py` does not import PyFlink, so its tests run in the regular `uv` environment.

`MAD` (`anomaly_detector/mad.py`):

- Needs at least 2 values; below that it raises `InsufficientDataError` from `anomaly_detector/errors.py`.
- Modified z-score: `|last - median| / (1.4826 × median absolute deviation)`, threshold `3.5`.
- When the median absolute deviation is 0 (over half the values equal the median), the scale is `1.253314 × mean absolute deviation` instead (Iglewicz & Hoaglin). When that is 0 too, every value is equal and the score is 0. The score is always finite, so it can be encoded as JSON.
- Returned numbers are Python `float`s; numpy scalars cannot be encoded by PyFlink's coders.

Input rows follow `Metric` (`shared/metrics.py`) and output rows follow `Anomaly` (`shared/anomalies.py`). `METRIC_FIELD_TYPES` and `ANOMALY_FIELD_TYPES` in `anomaly_detector/repository.py` must name the same fields as those dataclasses or the job raises `RuntimeError` at import.

### Anomaly record

Published on `anomalies` as JSON, without a Kafka key:

```json
{
  "metric_id": "01a0e974-dc94-74df-ae87-e0e6064b18ef",
  "machine_id": "server-42",
  "metric_name": "system.cpu.utilization",
  "timestamp": 1790595000.0,
  "value": 87.4,
  "algorithm": "mad",
  "score": 8.72,
  "threshold": 3.5,
  "direction": "up",
  "detected_at": 1790595000.8,
  "metric_attributes": {"core": "0"},
  "details": {"median": 21.3, "scale": 7.6, "window_size": 342.0},
  "schema_version": 1
}
```

- `metric_id` is the anomalous sample's id. With `machine_id`, `metric_name`, and `timestamp` it is that sample's key in `metrics`. `timestamp` is the sample time, not the detection time (`detected_at`); both are epoch seconds, like `raw_metrics`. `metric_attributes` repeats the sample's attributes so an anomaly reads on its own.
- `direction` is `up` when `value` is above the window median, else `down`. A name such as "cpu spike" is `metric_name` + `direction`.
- `algorithm` and `details` carry algorithm-specific numbers (`median`, `scale`, `window_size` for `mad`), so another algorithm fits without a schema change.
- `schema_version` changes only for a change that is not additive. Consumers ignore fields they do not know, so a producer may add a field first. JSON that omits `schema_version` is stored as `1`, the `Anomaly` default.

Job submission, the Kafka connector jar, and consumer-group rules are in [docs/operations.md](docs/operations.md).

## ClickHouse writers

`clickhouse_writer` moves one Kafka topic into one ClickHouse table. A `Sink` in `clickhouse_writer/repository.py` names the table, its columns, the record dataclass, and the row conversion. Two sinks exist, each with its own console script and consumer group:

| Script | Sink | Default topic | Default group | Table |
| --- | --- | --- | --- | --- |
| `metrics-writer` | `METRICS` | `raw_metrics` | `clickhouse-metrics-writer` | `metrics` |
| `anomalies-writer` | `ANOMALIES` | `anomalies` | `clickhouse-anomalies-writer` | `anomalies` |

`KAFKA_TOPIC` and `KAFKA_CONSUMER_GROUP_ID` override the defaults. Auto-commit is off. Offset reset is `earliest`.

Records are decoded as JSON into the sink's dataclass and converted to a ClickHouse row as they are polled; unknown keys are dropped. A record missing a required key or without a valid `metric_id` is logged as `Skipping malformed record` and left out of the batch. Its offset is committed with the next batch that inserts successfully. A prefix of such records, with no valid row after them, is read again after a restart.

A batch flushes when it reaches `WRITER_BATCH_SIZE` (default 100) or when `WRITER_BATCH_TIMEOUT_SECONDS` (default 5) has passed since the previous flush. `KafkaRecordsRepository.poll` waits up to 1 second. The timeout is checked when a poll returns, so a timeout under a second still waits for that poll. An empty poll does not write or commit. `SIGTERM` stops the loop and flushes a partial batch. `SIGINT` raises `KeyboardInterrupt` out of `run()` before that flush, and the open batch stays uncommitted.

`ClickHouseRepository.insert_batch` writes the table over HTTP (`clickhouse-connect`), then `ClickHouseWriter` commits offsets. A failed insert leaves the offsets uncommitted so the batch is replayed. ClickHouse has no `ON CONFLICT`: the replayed rows are inserted again and `ReplacingMergeTree` collapses rows with the same sorting key on merge, keeping the latest `ingested_at`. Until a merge runs, duplicates are visible; read with `FINAL` when that matters:

```sql
SELECT machine_id, name, count() FROM metrics FINAL GROUP BY machine_id, name
```

`None` maps are stored as empty maps. Epoch-second times are converted to UTC `datetime`s (`DateTime64(3, 'UTC')`). Sink columns are the dataclass fields. ClickHouse fills `ingested_at` with `DEFAULT now64(3)`, and `ReplacingMergeTree` keeps the row with the greatest `ingested_at`.

`attributes` and `metric_attributes` values are strings (`Map(String, String)`). `details` values are numbers (`Map(LowCardinality(String), Float64)`); ints are stored as floats. `parse_record` drops a record when JSON decoding, dataclass construction, or `uuid.UUID` raises `TypeError` or `ValueError`. Map values are checked later, inside `client.insert`:

- A non-string attribute value raises `AttributeError` (`str.encode`). A non-numeric `details` value, including `None`, raises `ValueError` or `TypeError` from `float()`. `insert_batch` catches `ClickHouseError` only, so the process exits and the uncommitted batch is read again on restart.
- `None` inside `attributes` or `metric_attributes` raises `DataError`, which is a `ClickHouseError` and follows the retry below.
- A map that is `None` as a whole is stored as `{}` before insert.

ClickHouse errors retry up to 5 times. Backoff is `backoff_base_seconds * 2^(attempt-1)` with a 1 second base (1s, 2s, 4s, 8s). The HTTP client holds no transaction, so the same client is reused.

## Database

[golang-migrate](https://github.com/golang-migrate/migrate) runs `db/migrations` against ClickHouse (`metrics` database) from the `migrate` Compose service. Applied versions are recorded in `metrics.schema_migrations`. Every migration is a numbered pair, `NNNNNN_<name>.up.sql` and `NNNNNN_<name>.down.sql`; create one with `make migrate-new name=<name>`.

| Migration | Effect |
| --- | --- |
| 000001 | `metrics` table, `ReplacingMergeTree`, hourly partitions, 24-hour TTL. |
| 000002 | `anomalies` table, same engine, partitioning, and TTL. |

`metrics` holds only what devices report. Columns: `metric_id` (`UUID`), `machine_id`, `name`, `timestamp`, `type`, `unit`, `value`, `attributes` (`Map(String, String)`), `ingested_at`. Sorting (and deduplication) key `(machine_id, name, timestamp, metric_id)`: queries filter by series and time, and `metric_id` makes each sample unique, including two series at the same instant that differ only in `attributes`. `ttl_only_drop_parts` drops whole expired hourly parts instead of rewriting them.

`anomalies` columns are the fields of the anomaly record plus `ingested_at`, with `details` as `Map(LowCardinality(String), Float64)`. Sorting key `(machine_id, metric_name, timestamp, metric_id, algorithm)`: the `metrics` key plus the algorithm.

The metric sample behind an anomaly:

```sql
SELECT a.metric_name, a.direction, a.score, m.value, m.unit, m.attributes
FROM anomalies AS a FINAL
JOIN metrics AS m FINAL
  ON  m.machine_id = a.machine_id AND m.name = a.metric_name
  AND m.timestamp = a.timestamp AND m.metric_id = a.metric_id
```

`metric_id` alone identifies the sample. The other three columns are in the condition because `metrics` is sorted by them; joining on `metric_id` alone reads the whole table. Both tables expire after 24 hours; `anomalies.value` and `metric_attributes` keep the sample's data regardless.

Identity trade-off: `metric_id` is assigned per collector. If two collectors ever scraped the same agent, the same reading would become two rows with different ids, where a key of `(machine_id, name, attributes, timestamp)` would have collapsed them. With one collector that does not happen.

The TimescaleDB schema (Flyway `V1`–`V5`) was removed along with TimescaleDB; it is in git history.

## Tests

Tests mirror the package directories (`tests/agent`, `tests/collector`, `tests/anomaly_detector`, `tests/clickhouse_writer`) and import `activityreporter.*`. `make test` runs `uv run pytest`.
