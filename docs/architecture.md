# Architecture

ActivityReporter collects CPU and memory metrics from my machines, scores every sample for anomalies, and shows both in Grafana. It is also where I learn Kafka, Flink, and ClickHouse, so some parts are bigger than a few home machines need. That is a deliberate choice, noted below where it applies.

```
agent (on each machine)       serves /v1/metrics and /v1/machine
   │  HTTP, every 10 s
collector                     finds agents over mDNS, scrapes them
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
| **agent** | FastAPI app on every machine. `/v1/metrics` returns CPU, memory, uptime, and battery when there is one; `/v1/machine` returns hostname, OS, cores, memory, and disk. | Each machine only has to answer HTTP. It knows nothing about Kafka or the database. |
| **collector** | Scrapes every agent every 10 s, fetches machine info every 5 min, and publishes both to Kafka. Gives each sample a `metric_id` (UUIDv7). | One place decides when and what to collect. `metric_id` follows the sample everywhere after this. |
| **Kafka** | Topics `raw_metrics`, `machines`, `evaluations`. | Decouples collecting from scoring from storing: each can be down or replayed without losing data. *Learning goal.* |
| **Flink job** | Keeps the last hour of each series (`machine_id`, `name`) and emits one evaluation per sample per algorithm. Runs in Application Mode: the jobmanager starts the job itself. | Stateful stream processing per series. *Learning goal*: a plain Kafka consumer would do at this scale. |
| **clickhouse-writer** | One process, one writer per topic (`metrics`, `evaluations`, `machines`) on its own thread, sharing the `TopicWriter` base class. Each batches rows and commits its offsets only after its table is written. One writer raising stops the other two and the process exits. | A topic's offsets, batches and log lines are its writer's alone, so a problem is easy to place. Replays are safe because the tables deduplicate. Compose restarts that one container. |
| **ClickHouse** | Tables `metrics`, `evaluations`, `machines`. Schema in `db/migrations`. | Fast time-range queries for Grafana, and a good fit for append-only data. |
| **Grafana** | Dashboard `machine-usage-metrics.json`, provisioned on start. | Charts without writing a frontend. |

## Data model

- **`metrics`**: one row per sample, as the agent reported it.
- **`evaluations`**: one row per sample *per algorithm*, anomalous or not, with only what the dashboard draws: `baseline`, `lower`, `upper`, `is_anomaly`. It's linked to its sample by `metric_id`, and the value stays in `metrics`. A new algorithm is a new `algorithm` value, not a schema change.
- **`machines`**: the latest description of each machine, joined to the other two on `machine_id`.

`metrics` and `evaluations` keep 24 hours; `machines` keeps everything. All three are `ReplacingMergeTree`, so a replayed batch collapses to one row per key on merge.

## Anomaly detection

Two algorithms score every gauge except battery, `system.memory.limit`, and `system.uptime`:

- **MAD**: how far the value is from the median of the last hour, in units of the typical spread. Flags both directions. Needs 20 samples.
- **EWMA**: how far the value rises above a moving average. Flags only rises. Needs 11 samples.

On its own, each one flags tiny steps in a series that barely moves, so each has a minimum deviation per metric (`METRIC_DETECTORS` in `anomaly_detector/detection.py`):

| Metric | Smallest deviation that can be flagged |
| --- | --- |
| `system.cpu.utilization` | 10 percentage points |
| `system.memory.usage` | 2 % of its level |
| anything else | 1 % of its level |

To add an algorithm: write a class in `anomaly_detector/detectors.py` that returns the shared result fields, and add it to the detector lists. The table and dashboard don't change.

## Running it

- `make up` starts the stack; `make agent` runs an agent on this machine.
- `WAIT=300 make smoke` checks every step end to end.
- After a change to the Flink job: `docker compose restart jobmanager`.

Details are in [README](../README.md) and [operations](operations.md).

## Not doing yet

On purpose, until there's a reason:

- **Flink checkpointing.** A restarted job starts with empty windows and rescores from the start of the topic; the duplicates collapse.
- **Alerting.** Anomalies are only visible on the dashboard.
- **Anomaly episodes.** Each anomalous sample is its own row; consecutive ones aren't grouped into one event.
- **Long-term storage and downsampling.** Everything older than 24 hours is gone.
- **Authentication** on the agent, and secrets outside `docker-compose.yml`.

## Known problems

- **The collector doesn't always find agents.** mDNS needs the collector on the host network (`network_mode: host`). That only works with Docker Engine running directly on Linux, not with Docker Desktop, and a firewall can block it. Workaround: list agents in `COLLECTOR_ENDPOINTS`. Candidate fix: drop mDNS and always use that list.
