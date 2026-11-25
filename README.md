# activity-metrics

[![CI](https://github.com/zabukovecitdev/activity-metrics/actions/workflows/ci.yml/badge.svg)](https://github.com/zabukovecitdev/activity-metrics/actions/workflows/ci.yml)

activity-metrics collects CPU and memory metrics from a few machines, scores every sample for anomalies with two
detectors (MAD and EWMA), and charts the samples, the detectors' baselines and bands, and the anomalies in Grafana. It
exists to learn Kafka, Flink and ClickHouse on real data, so the pipeline is bigger than a few home machines need. That
is a deliberate choice, recorded in [ADR 0001](docs/adr/0001-kafka-and-flink-as-a-learning-goal.md).

```
agent ──HTTP──▶ collector ─┬─▶ Kafka raw_metrics ─┬───────────────────────────────────────────────────┐
                           │                      └─▶ anomaly_detector (Flink) ──▶ Kafka evaluations ─┼─▶ clickhouse-writer ──▶ ClickHouse metrics, evaluations, machines
                           └─▶ Kafka machines ────────────────────────────────────────────────────────┘
```

## Quickstart

Needs Docker with Compose, and [uv](https://docs.astral.sh/uv/) for the agent.

```bash
make up        # builds the images and starts the stack; the migrate service applies the migrations
make migrate   # applies pending ClickHouse migrations, if make up did not
make agent     # runs an agent on this machine, on port 8080
```

Open Grafana at http://localhost:3000 (`admin` / `admin`) and pick the *Metrics explorer* dashboard. The collector
scrapes the agents in `collector.toml`. Its default entry, `http://host.docker.internal:8080`, is the agent from
`make agent`. EWMA scores a series from its 11th sample and MAD from its 20th, about 20 seconds at the collector's
default 1-second gap.

`WAIT=300 make smoke` checks every step, from the containers to the dashboard, and retries for up to 5 minutes.

## Commands

| Command                   | Does                                                                     |
|---------------------------|--------------------------------------------------------------------------|
| `make up`                 | (re)starts the whole stack in Docker; Kafka is reset, ClickHouse is kept |
| `make down`               | stops the stack                                                          |
| `make migrate`            | applies pending ClickHouse migrations                                    |
| `make migrate-down`       | reverts the latest migration                                             |
| `make migrate-new name=x` | creates the next numbered `up`/`down` migration pair                     |
| `make agent`              | runs the agent locally                                                   |
| `make collector`          | runs the collector locally                                               |
| `make clickhouse-writer`  | runs the ClickHouse writers locally                                      |
| `make anomaly-detector`   | runs the Flink job locally (creates `flink/.venv` with Python 3.11)      |
| `make venv`               | opens a subshell with the project's virtual environment active          |
| `make test`               | runs the tests                                                           |
| `make lint`               | runs `ruff check` and `ruff format --check`                              |
| `make format`             | formats the code with `ruff format`                                      |
| `make typecheck`          | runs `pyright`                                                           |
| `make complexity`         | runs `complexipy` (cognitive complexity, max 15 per function)            |
| `make check`              | runs lint, typecheck, complexity and tests, as CI does                   |
| `make mutate`             | mutation-tests the detectors (`uv run mutmut run`)                       |
| `make smoke`              | checks a running stack end to end (`WAIT=300 make smoke` retries)        |

In Docker, the Flink jobmanager runs in Application Mode: it starts the anomaly detection job itself, from `./src`. To
run a code change, `docker compose restart jobmanager`. The Flink UI is at http://localhost:8081.

## Design decisions

Why Kafka and Flink, why the collector pulls, why a 360-value window, what the delivery guarantee is, and the other
choices behind the pipeline are in the [architecture decision records](docs/adr/README.md).

## Reference

- [docs/architecture.md](docs/architecture.md): the components, the data model and what is not done yet.
- [docs/reference.md](docs/reference.md): the package layout, the HTTP API, the Kafka records, the detectors, the
  ClickHouse tables, the dashboards and the tests.
- [docs/operations.md](docs/operations.md): environment variables, Compose, the Flink job, migrations, packaging and
  troubleshooting.
