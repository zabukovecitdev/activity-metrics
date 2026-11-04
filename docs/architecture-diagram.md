# Architecture diagram

Data flows top to bottom. Solid arrows carry data. Dotted arrows carry errors or scrapes.

```mermaid
flowchart TB
  subgraph Hosts["Each monitored machine"]
    agent["agent<br/>/v1/metrics, /v1/machine"]
  end

  collector["collector<br/>scrapes agents, adds metric_id"]

  subgraph Kafka["Kafka"]
    direction LR
    raw["raw_metrics"]
    machines_t["machines"]
    evals["evaluations"]
    dlq["*_dlq<br/>invalid input"]
  end

  flink["Flink job (Application Mode)<br/>MAD and EWMA per series"]
  writer["clickhouse-writer<br/>one sink per topic"]
  ch[("ClickHouse<br/>metrics, evaluations, machines")]

  subgraph Monitoring["Dashboards"]
    direction LR
    prom["Prometheus"]
    grafana["Grafana"]
  end

  agent -- "HTTP, 1 s gap" --> collector
  collector -- "raw_metrics" --> raw
  collector -- "machines, every 5 min" --> machines_t
  collector -.-> dlq
  raw --> flink
  flink -- "evaluations" --> evals
  raw --> writer
  evals --> writer
  machines_t --> writer
  writer --> ch
  ch --> grafana
  collector -.-> prom
  writer -.-> prom
  flink -.-> prom
  prom --> grafana
```

## Notes

- The collector sends invalid input to `<topic>_dlq`.
- Prometheus scrapes the collector and clickhouse-writer on port 8000, and Flink on port 9249.
- Each sample keeps its `metric_id` from the collector to ClickHouse.
- The text description of these components is in [architecture](architecture.md).
