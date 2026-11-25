# Architecture

activity-metrics collects CPU and memory metrics from my machines, scores every sample for anomalies, and shows both in Grafana. It is also where I learn Kafka, Flink, and ClickHouse, so some parts are bigger than a few home machines need. That is a deliberate choice, noted below where it applies.

The reasons behind each design choice are in the [architecture decision records](adr/README.md).

```
agent (on each machine)       serves /v1/metrics and /v1/machine
   │  HTTP, 1 s gap between rounds
collector                     scrapes the agents listed in collector.toml
   │  Kafka: raw_metrics, machines
Flink job                     scores each sample with MAD and EWMA
   │  Kafka: evaluations
clickhouse-writer             one thread per topic, each writes its table
   │  ClickHouse: metrics, evaluations, machines
Grafana                       pick a host, see its series and anomalies
```

## Components

| Component | What it does | Why it exists |
| --- | --- | --- |
| **agent** | FastAPI app on every machine. `/v1/metrics` returns CPU, memory, and uptime; `/v1/machine` returns hostname, OS, cores, memory, and disk. | Each machine only has to answer HTTP. It knows nothing about Kafka or the database. |
| **collector** | Scrapes every agent, then waits 1 s (`COLLECTOR_SCRAPE_INTERVAL_SECONDS`), fetches machine info every 5 min, and publishes both to Kafka. Gives each sample a `metric_id` (UUIDv7). | One place decides when and what to collect. `metric_id` follows the sample everywhere after this. |
| **Kafka** | Topics `raw_metrics`, `machines`, `evaluations`. | Decouples collecting from scoring from storing: each can be down or replayed without losing data. *Learning goal* ([ADR 0001](adr/0001-kafka-and-flink-as-a-learning-goal.md)). |
| **Flink job** | Keeps the last 360 values of each series (`machine_id`, `name`), in arrival order, with no time cutoff, and emits one evaluation per sample per algorithm. Runs in Application Mode: the jobmanager starts the job itself. | Stateful stream processing per series. A count stays full at any sample rate. *Learning goal*: a plain Kafka consumer would do at this scale ([ADR 0001](adr/0001-kafka-and-flink-as-a-learning-goal.md)). |
| **clickhouse-writer** | One process, one sink per topic (`metrics`, `evaluations`, `machines`) on its own thread, sharing the `ClickHouseSinkBase` base class. Each batches rows and commits its offsets only after its table is written. | A topic's offsets, batches and log lines are its sink's alone, so a problem is easy to place. Replays are safe because the tables deduplicate. See [ADR 0010](adr/0010-a-consumer-and-client-per-sink.md) and [ADR 0011](adr/0011-a-custom-writer-not-the-kafka-table-engine.md). |
| **ClickHouse** | Tables `metrics`, `evaluations`, `machines`. Schema in `db/migrations`. | Fast time-range queries for Grafana, and a good fit for append-only data. |
| **Grafana** | Dashboards `machine-usage-metrics.json` (metrics explorer) and `pipeline-performance.json`, provisioned on start. | Charts without writing a frontend. |

## Data model

- **`metrics`**: one row per sample, as the agent reported it.
- **`evaluations`**: one row per sample *per algorithm*, anomalous or not, with only what the dashboard draws: `baseline`, `lower`, `upper`, `is_anomaly`. It's linked to its sample by `metric_id`, and the value stays in `metrics`. A new algorithm is a new `algorithm` value, not a schema change.
- **`machines`**: the latest description of each machine, joined to the other two on `machine_id`.

`metrics` and `evaluations` keep 24 hours; `machines` keeps everything ([ADR 0013](adr/0013-24-hour-retention.md)). All three are `ReplacingMergeTree`, so a replayed batch collapses to one row per key on merge.

## Anomaly detection

Two algorithms score CPU, memory usage, and any series whose name starts with `nab.`:

- **MAD**: how far the value is from the median of the last 360 values, in units of the typical spread. Flags both directions. Needs 20 samples.
- **EWMA**: how far the value rises above a moving average of those same values. Flags only rises. Needs 11 samples. With both floors at 0 and a flat history, any rise is flagged and the upper band sits on the average. The detector does not divide by a scale of 0.

The 360 values are the samples the job has seen. A pause leaves them in place until newer samples push them out. They cover a little over six minutes at the collector's default 1-second gap, one hour at a steady 10-second cadence, and 30 hours of 5-minute data. An hour of 5-minute data is 12 values, which is why MAD used to score none of a NAB replay.

On its own, each algorithm flags tiny steps in a series that barely moves, so CPU and memory have a minimum deviation (`MetricEvaluator.DETECTORS` in `anomaly_detector/services/metric_evaluator.py`):

| Metric | Smallest deviation that can be flagged |
| --- | --- |
| `system.cpu.utilization` | 10 percentage points |
| `system.memory.usage` | 2 % of its level |
| `nab.<file name>` | none (`NAB_DETECTORS`: `MAD()` and `EWMA(min_deviation=0.0)`) |

NAB values run from about 0.1 to millions, so a floor in the values' units cannot fit every file. The prefix is case-sensitive and includes the dot.

To add an algorithm: write a class in `anomaly_detector/algorithms/` that returns the shared result fields, and add it to the detector lists. The table and dashboard don't change.

## Running it

- `make up` starts the stack; `make agent` runs an agent on this machine.
- `WAIT=300 make smoke` checks every step end to end.
- After a change to the Flink job: `docker compose restart jobmanager`.

Details are in [reference](reference.md) and [operations](operations.md).

## Not doing yet

On purpose, until there's a reason. Each item names what would make it worth doing.

- **End-to-end exactly-once.** Delivery is at-least-once and ClickHouse collapses the repeats; see [ADR 0004](adr/0004-at-least-once-delivery.md). Worth doing when something reads `evaluations` or the tables without deduplicating, such as a consumer that acts on each record or a query that cannot use `FINAL`.
- **Alerting.** Anomalies are only visible on the dashboard. Worth doing when someone other than me needs to know about an anomaly, or when I want to know without opening Grafana.
- **Anomaly episodes.** Each anomalous sample is its own row; consecutive ones aren't grouped into one event. Worth doing together with alerting, so one CPU spike sends one alert and not one per sample, or when I want to count incidents instead of samples.
- **Long-term storage and downsampling.** Everything older than 24 hours is gone; see [ADR 0013](adr/0013-24-hour-retention.md). Worth doing when I want to compare a series with more than a day ago, such as the same hour last week.
- **Authentication** on the agent, and secrets outside `docker-compose.yml`. Worth doing when an agent listens on a network I do not control, or the stack runs anywhere but my own machines.
