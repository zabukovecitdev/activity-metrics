# ActivityReporter

```
agent ──HTTP──▶ collector ─┬─▶ Kafka raw_metrics ─┬───────────────────────────────────────────────────┐
                           │                      └─▶ anomaly_detector (Flink) ──▶ Kafka evaluations ─┼─▶ clickhouse-writer ──▶ ClickHouse metrics, evaluations, machines
                           └─▶ Kafka machines ────────────────────────────────────────────────────────┘
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
  shared/            code used by more than one application: Metric, Machine, Evaluation, mDNS constants
  agent/             runs on every machine, serves /v1/metrics and /v1/machine
  collector/         scrapes agents, publishes to raw_metrics and machines
  anomaly_detector/  PyFlink job, raw_metrics → evaluations (every scored sample, once per algorithm)
  clickhouse_writer/ one consumer per topic, Kafka topic → ClickHouse table: raw_metrics → metrics,
                     evaluations → evaluations, machines → machines
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
| `make clickhouse-writer` | runs the ClickHouse writers locally                                 |
| `make anomaly-detector` | runs the Flink job locally (creates `flink/.venv` with Python 3.11)  |
| `make test`             | runs the tests                                                       |
| `make smoke`            | checks a running stack end to end (`WAIT=300 make smoke` retries)    |

In Docker, the Flink jobmanager runs in Application Mode: it starts the anomaly detection job itself, from
`./src`. To run a code change, `docker compose restart jobmanager`. The Flink UI is at http://localhost:8081.

Environment variables, Compose details, and troubleshooting are in [docs/operations.md](docs/operations.md).

## HTTP API

Interactive docs: `GET /` redirects to `/docs`.

| Method | Path | Body |
| --- | --- | --- |
| `GET` | `/v1/health` | `{"status": "ok"}` |
| `GET` | `/v1/machine` | `Machine` from `shared/models/machine.py` |
| `GET` | `/v1/metrics` | One snapshot of current gauges |

`/v1/machine` fields: `machine_id`, `hostname`, `os` (lowercased `platform.system()`), `os_version`, `architecture`, `cores` (`0` when `psutil.cpu_count()` can't tell), `total_memory` and `total_disk` (bytes, disk of the root filesystem), `last_boot` (UTC ISO-8601), `observed_at` (epoch seconds when the agent answered). `machine_id` comes from `machineid.id()`. Only fields that rarely change are here; uptime is the `system.uptime` metric.

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
| `system.uptime` | `s` | Always. Seconds since `psutil.boot_time()`. |
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

Required JSON fields: `machine_id` (string), `timestamp` (ISO-8601), `metrics` (array of objects with `name`, `type`, `unit`, `value`). `attributes` is optional. The shared timestamp is converted with `datetime.fromisoformat(...).timestamp()` and copied onto every `Metric`. `value` is cast with `float()`. Each `Metric` gets its own `metric_id`, a UUIDv7 string from `uuid.uuid7()`: the sample's identity from here on, in Kafka, in `metrics`, and in its `evaluations`. The agent does not send one.

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

### Machine info

On the first scrape of an endpoint, and then every 300 seconds (`MACHINE_REFRESH_SECONDS`), the collector also GETs `/v1/machine` from the same host and port (`machine_url` swaps the metrics path for `/v1/machine`). The `Machine` is published as JSON on `KAFKA_MACHINES_TOPIC` (default `machines`), keyed by `machine_id`, with the same producer settings as metrics. A failed fetch or publish is logged and retried on the next scrape; it never stops that endpoint's metrics. A response missing a field, or with an empty `machine_id`, is logged as `Malformed machine info from ...`.

## Anomaly detection

`anomaly_detector/main.py` is the PyFlink job `Anomaly Detection`.

- Source topic: `KAFKA_RAW_METRICS_TOPIC` (default `raw_metrics`), consumer group `anomaly-detector`, starting offset `earliest`.
- Sink topic: `KAFKA_EVALUATIONS_TOPIC` (default `evaluations`). Every scored sample produces one evaluation per detector, anomalous or not, so the baseline and bands can be drawn next to the raw series.
- Event time is `timestamp * 1000` milliseconds. Out-of-orderness bound is 5 seconds. Kafka record timestamps are not used.
- Parallelism is 2. The stream is keyed by `(machine_id, name)` so each series keeps its own state. A single-partition source leaves one Kafka source subtask idle; the split applies after `key_by`.
- State is the last hour of `(event_time, value)` pairs for that key (`WINDOW_MS`).

Scoring rules (`anomaly_detector/detection.py`, called from `AnomalyDetector.process_element` in `service.py`):

- `type` other than `gauge`, and the names `system.battery.charging`, `system.battery.utilization`, `system.memory.limit`, and `system.uptime`, are dropped without state. Those series are flags, values that barely move, or ever-growing counters, so every small step would be flagged.
- `detectors_for(name)` picks the detectors for the series: `METRIC_DETECTORS` for a metric listed there, `DEFAULT_DETECTORS` otherwise. Each scores the latest value against the window once the window has `min_points` values. Until then that detector emits nothing for the series.

| Metric | `mad` | `ewma` |
| --- | --- | --- |
| `system.cpu.utilization` | `min_deviation=10.0` (percentage points) | `min_deviation=10.0` |
| `system.memory.usage` | `min_relative=0.02` (2% of the median) | `min_relative=0.02` (2% of the average) |
| any other scored gauge | `min_relative=0.01` | `min_relative=0.01` |

`detection.py` and `detectors.py` do not import PyFlink, so their tests run in the regular `uv` environment.

### Detectors

A detector (`anomaly_detector/detectors.py`) has a `name`, `min_points`, and `evaluate(window) -> Result`. `Result` is what the dashboard draws, in the value's units:

| Field | `mad` | `ewma` |
| --- | --- | --- |
| `baseline` | window median | exponentially weighted average |
| `lower` / `upper` | median ∓ `THRESHOLD × scale` | `None` / average + `THRESHOLD × scale` (only rises are flagged) |
| `is_anomaly` | modified z-score `>= 3.5` | deviation ÷ scale `> 1.0` |
| `min_points` | 20 | 11 (`EMWA.WARMUP_READINGS + 1`) |

The score and threshold behind `is_anomaly` stay inside the detector; only the flag is published. A detector without a band returns `None` for it, which becomes `NULL` in ClickHouse and a gap in Grafana.

To add an algorithm, write a class with that shape and add it to `DEFAULT_DETECTORS` and the lists in `METRIC_DETECTORS`. No schema, writer, or dashboard change is needed: its anomalies show up on every chart, labelled with its name.

`MAD` (`anomaly_detector/mad.py`):

- Needs at least 2 values; below that it raises `InsufficientDataError` from `anomaly_detector/errors.py`.
- Modified z-score: `|last - median| / (1.4826 × median absolute deviation)`, threshold `3.5`.
- When the median absolute deviation is 0 (over half the values equal the median), the scale is `1.253314 × mean absolute deviation` instead (Iglewicz & Hoaglin). When that is 0 too, every value is equal and the score is 0. The score is always finite, so it can be encoded as JSON.
- The scale has a floor, `max(min_deviation, min_relative × |median|) / THRESHOLD`, so a deviation smaller than `min_deviation` and than `min_relative` of the median never reaches the threshold. Without it, a series that barely moves has a tiny MAD and every step is flagged: memory in use changing by 32 KB on a 470 MB median scored 8, against a threshold of 3.5. Both default to 0, the textbook modified z-score.

To tune the floors, count what they let through, for example `SELECT metric_name, algorithm, toDate(timestamp) AS day, countIf(is_anomaly) FROM evaluations FINAL GROUP BY ALL`. At one sample every 10 seconds a series has 8,640 a day.
- Returned numbers are Python `float`s; numpy scalars cannot be encoded by PyFlink's coders.

`EMWA` (`anomaly_detector/emwa.py`) walks the window with `alpha = 0.1`, scoring each value against the average before it. The scale is `max(3 × standard deviation, min_deviation, min_relative × |average|)`; the floors default to 10 and 0 and are set per metric above, since 10 means percentage points for CPU but bytes for memory. Values flagged after the 10-reading warm-up move the average with a damped `alpha`, so a spike does not drag the baseline along.

Input rows follow `Metric` (`shared/models/metric.py`) and output rows follow `Evaluation` (`shared/models/evaluation.py`). `METRIC_FIELD_TYPES` and `EVALUATION_FIELD_TYPES` in `anomaly_detector/repository.py` must name the same fields as those dataclasses or the job raises `RuntimeError` at import. The Flink image runs Python 3.10, so code under `anomaly_detector/` and `shared/` must not use newer syntax.

### Evaluation record

Published on `evaluations` as JSON, without a Kafka key:

```json
{
  "metric_id": "01a0e974-dc94-74df-ae87-e0e6064b18ef",
  "machine_id": "server-42",
  "metric_name": "system.cpu.utilization",
  "timestamp": 1790595000.0,
  "algorithm": "ewma",
  "baseline": 21.3,
  "lower": null,
  "upper": 31.3,
  "is_anomaly": true
}
```

- `metric_id` is the scored sample's id in `metrics`, which holds its value; it isn't repeated here. `machine_id`, `metric_name`, and `timestamp` (epoch seconds, like `raw_metrics`) are the sample's series and time, so the table is sorted like `metrics`.
- Numbers are stored as computed, at full `Float64` precision.

Job submission, the Kafka connector jar, and consumer-group rules are in [docs/operations.md](docs/operations.md).

## ClickHouse writers

`clickhouse-writer` is one process that runs one writer per topic on its own thread, each with its own Kafka consumer and ClickHouse client, so each topic has its own offsets and its own batches. `TopicWriter` in `clickhouse_writer/service.py` is the abstract base: it polls, converts, batches, writes and commits. A subclass in `clickhouse_writer/writers.py` names the table, its columns, the record dataclass, the topic, and implements `to_row`. A record is converted by the writer of the topic it came from:

| Writer | Topic variable | Default topic | Table | Group |
| --- | --- | --- | --- | --- |
| `MetricsWriter` | `KAFKA_RAW_METRICS_TOPIC` | `raw_metrics` | `metrics` | `clickhouse-metrics-writer` |
| `EvaluationsWriter` | `KAFKA_EVALUATIONS_TOPIC` | `evaluations` | `evaluations` | `clickhouse-evaluations-writer` |
| `MachinesWriter` | `KAFKA_MACHINES_TOPIC` | `machines` | `machines` | `clickhouse-machines-writer` |

The topic variables are the ones the producers read, so a renamed topic is set once. Auto-commit is off. Offset reset is `earliest`. To write another topic, subclass `TopicWriter` and add it to `WRITERS`. A writer that fails after its retries stops the other two and the process exits, so Compose restarts all three. To scale a topic, start more `clickhouse-writer` processes: they share each topic's group, so Kafka splits the partitions between them.

Records are decoded as JSON into the writer's record dataclass and converted to a ClickHouse row as they are polled; unknown keys are dropped. A record missing a required key, a metric or evaluation without a valid `metric_id`, an evaluation or machine with an empty `machine_id`, or a machine whose `last_boot` has no UTC offset is logged as `Skipping malformed record` and skipped there, so it cannot fail its batch on every retry. A flush happens when the batch reaches `WRITER_BATCH_SIZE` (default 100) or when `WRITER_BATCH_TIMEOUT_SECONDS` (default 5) has passed since the previous flush. `SIGTERM` stops the loop and flushes a partial batch. An empty poll does not write or commit.

`ClickHouseRepository.insert_batch` writes each table over HTTP (`clickhouse-connect`), then `ClickHouseWriter` commits offsets. Offsets belong to the consumer, not a table, so the commit waits until every table in the flush is written; a failed insert leaves them uncommitted and the whole flush is replayed, including tables that were already written. ClickHouse has no `ON CONFLICT`: the replayed rows are inserted again and `ReplacingMergeTree` collapses rows with the same sorting key on merge, keeping the latest `ingested_at`. Until a merge runs, duplicates are visible; read with `FINAL` when that matters:

```sql
SELECT machine_id, name, count() FROM metrics FINAL GROUP BY machine_id, name
```

`None` maps are stored as empty maps and `None` bands as `NULL`. Epoch-second and ISO-8601 times are converted to UTC `datetime`s (`DateTime64(3, 'UTC')`).

ClickHouse errors retry up to 5 times. Backoff is `backoff_base_seconds * 2^(attempt-1)` with a 1 second base (1s, 2s, 4s, 8s). The HTTP client holds no transaction, so the same client is reused.

## Database

[golang-migrate](https://github.com/golang-migrate/migrate) runs `db/migrations` against ClickHouse (`metrics` database) from the `migrate` Compose service. Applied versions are recorded in `metrics.schema_migrations`. Every migration is a numbered pair, `NNNNNN_<name>.up.sql` and `NNNNNN_<name>.down.sql`; create one with `make migrate-new name=<name>`. Statements are split on every `;`, comments included, so comments in a migration must not contain one.

| Migration | Effect |
| --- | --- |
| 000001 | `metrics` table, `ReplacingMergeTree`, hourly partitions, 24-hour TTL. |
| 000002 | `anomalies` table, same engine, partitioning, and TTL. |
| 000003 | `evaluations` table, same engine, partitioning, and TTL. |
| 000004 | `machines` table, `ReplacingMergeTree(observed_at)`, no TTL. |
| 000005 | Drops `anomalies`; its `down` recreates it. |
| 000006 | Replaces `evaluations` with the 9 columns the dashboard needs; its `down` recreates the 000003 table. |

`metrics` holds only what devices report. Columns: `metric_id` (`UUID`), `machine_id`, `name`, `timestamp`, `type`, `unit`, `value`, `attributes` (`Map(String, String)`), `ingested_at`. Sorting (and deduplication) key `(machine_id, name, timestamp, metric_id)`: queries filter by series and time, and `metric_id` makes each sample unique, including two series at the same instant that differ only in `attributes`. `ttl_only_drop_parts` drops whole expired hourly parts instead of rewriting them.

`evaluations` columns are the fields of the evaluation record: `metric_id` is `UUID`, `baseline` and `upper` are `Float64`, `lower` is `Nullable(Float64)`, `is_anomaly` is `Bool`. Sorting (and deduplication) key `(machine_id, metric_name, algorithm, timestamp, metric_id)`: one algorithm's results for a series over time are contiguous, and a replayed evaluation collapses on merge. Anomalous samples are `WHERE is_anomaly`.

`machines` holds the latest `Machine` per `machine_id`; `ReplacingMergeTree(observed_at)` keeps the newest on merge, so read it with `FINAL`. It has no TTL: a machine that stops reporting keeps its row, and `observed_at` is when it was last seen.

A sample with its evaluation, and a series with its hostname:

```sql
SELECT m.timestamp, m.value, e.baseline, e.lower, e.upper, e.is_anomaly
FROM (SELECT metric_id, timestamp, value FROM metrics FINAL
      WHERE machine_id = 'server-42' AND name = 'system.cpu.utilization') AS m
LEFT JOIN (SELECT metric_id, baseline, lower, upper, is_anomaly FROM evaluations FINAL
           WHERE machine_id = 'server-42' AND metric_name = 'system.cpu.utilization' AND algorithm = 'mad') AS e
  USING (metric_id)
ORDER BY m.timestamp
SETTINGS join_use_nulls = 1;

SELECT h.hostname, m.timestamp, m.value
FROM (SELECT machine_id, timestamp, value FROM metrics FINAL WHERE name = 'system.cpu.utilization') AS m
LEFT JOIN (SELECT machine_id, hostname FROM machines FINAL) AS h USING (machine_id);
```

`metric_id` alone identifies the sample. Filter both sides by series before joining, as above, or the join reads whole tables. `join_use_nulls = 1` makes samples without an evaluation (warm-up, unscored series) come back with `NULL` bands rather than ClickHouse's default `0`.

Identity trade-off: `metric_id` is assigned per collector. If two collectors ever scraped the same agent, the same reading would become two rows with different ids, where a key of `(machine_id, name, attributes, timestamp)` would have collapsed them. With one collector that does not happen.

The TimescaleDB schema (Flyway `V1`–`V5`) was removed along with TimescaleDB; it is in git history.

## Grafana

`docker/grafana/dashboards/machine-usage-metrics.json` is provisioned as *Machine usage metrics*. It uses only the metrics the agent already reports. Variables:

- `host` (shown as *Machine*): the hostname from `machines`, or the machine id for a machine the collector hasn't described yet.
- `machine_id`: hidden, the id of the selected host; every panel filters on it. Two machines with the same hostname would both map to the first id, so give them different hostnames.
- `metric_name` (*Explorer metric*): the metrics a detector has scored for that machine.

Panels:

- **Machine**: hostname, OS, architecture, cores, memory, disk, last boot, and last seen, from `machines`. **Uptime** from `system.uptime`.
- **CPU** and **Memory** gauges (memory is `system.memory.usage / system.memory.limit`), **Memory used**, and **Anomalies**: anomalous evaluations of every metric and algorithm in the time range.
- **CPU usage** and **Memory usage** (with the limit dashed): the series with every algorithm's anomalies as points, one colour per algorithm (query `B`, split by the `algorithm` column). Where two algorithms flag the same sample, their points overlap; the tooltip lists both.
- **Detector explorer** (`${metric_name}`): the series, every algorithm's anomalies, and every algorithm's `baseline`, `lower`, and `upper` as dashed lines; then a table of the last 200 anomalies with their values.

Battery series are still collected when a machine has a battery, but the dashboard doesn't show them.

## Tests

Tests mirror the package directories (`tests/agent`, `tests/collector`, `tests/anomaly_detector`, `tests/clickhouse_writer`) and import `activityreporter.*`. `make test` runs `uv run pytest`.
