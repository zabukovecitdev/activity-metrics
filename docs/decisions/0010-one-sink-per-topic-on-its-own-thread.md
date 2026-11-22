# 0010. One sink per topic, each on its own thread

Status: Accepted

Source: commits 6ef21a1 and 312b421, README.md (ClickHouse sinks), docs/architecture.md (clickhouse-writer row), docs/simplification-plan.md (question 4, "Don't merge the three writers").

## Context

Separate writers were merged into one process in 6ef21a1, with one consumer on all three topics and offsets committed after every table in a flush had been written. docs/architecture.md now describes a different shape.

## Decision

`clickhouse-writer` is one process that runs a sink per topic, each on its own thread with its own Kafka consumer, ClickHouse client and consumer group. Reason recorded in docs/architecture.md: a topic's offsets, batches and log lines are its sink's alone, so a problem is easy to place. The name "sink" is borrowed from Kafka Connect and Flink (312b421). The commit that moved from one consumer to three threads is not in this history; the reasoning above is from the docs.

## Consequences

- Three consumers and three clients instead of one each (plan, question 4).
- A sink that fails after its retries stops the other two and the process exits; Compose restarts all three (README.md).
- To load another topic, subclass `ClickHouseSinkBase` and add it to `SINKS` in `clickhouse_writer/main.py`.
- To scale, start more processes: they share each topic's group and Kafka splits the partitions.
