# Architecture Decision Records

A decision the code cannot show is recorded here. Each record names the code it is about, so `grep` for a symbol finds
its reasons. A code comment that would need more than a line to explain a decision points to its record instead.

| ADR | Decision |
|---|---|
| [0001](0001-kafka-and-flink-as-a-learning-goal.md) | Kafka and Flink for a few machines, as a learning goal |
| [0002](0002-pull-based-collection.md) | Pull-based collection: agents answer HTTP, the collector scrapes them |
| [0003](0003-count-based-window.md) | A window of the last 360 values, not a time window |
| [0004](0004-at-least-once-delivery.md) | At-least-once delivery, duplicates collapsed by `ReplacingMergeTree` |
| [0005](0005-daily-partitions.md) | Daily ClickHouse partitions, not hourly |
| [0006](0006-python-3-10-in-the-flink-image.md) | Python 3.10 in the Flink image, 3.14 everywhere else |
| [0007](0007-metric-id-as-sample-identity.md) | `metric_id` (UUIDv7) as the identity of a sample |
| [0008](0008-mad-and-ewma-with-floors.md) | MAD and EWMA, with absolute and relative floors |
| [0009](0009-an-evaluation-for-every-scored-sample.md) | Every scored sample produces an evaluation, anomalous or not |

New record: copy the shape of an existing one (Status, Context, Decision, Consequences) and add a row above.
