# 0001. Kafka between every stage

Status: Accepted

Source: docs/architecture.md (Components, Kafka row), README.md (diagram), docs/simplification-plan.md (questions 1, 6).

## Context

The pipeline is agent, collector, scorer and writer. Each could call the next one directly. The project is also where the author learns Kafka, Flink and ClickHouse, so "some parts are bigger than a few home machines need" (docs/architecture.md).

## Decision

Every hand-off between the collector, the Flink job and the ClickHouse writer goes through a Kafka topic: `raw_metrics`, `machines`, `evaluations`. Machine info also goes through Kafka, so the collector never touches the database (plan, question 6: "one path for all data").

## Consequences

- Collecting, scoring and storing can each be down or replayed without losing data (docs/architecture.md).
- Each reader has its own consumer group and starts at `earliest`, so a new reader replays what Kafka still holds (docs/operations.md).
- More moving parts than a few machines need. This is a stated learning goal (docs/architecture.md).
- Whether a simpler design was weighed: Not recorded.
