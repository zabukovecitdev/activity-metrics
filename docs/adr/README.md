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
| [0010](0010-a-consumer-and-client-per-sink.md) | One Kafka consumer and one ClickHouse client per sink, each on its own thread |
| [0011](0011-a-custom-writer-not-the-kafka-table-engine.md) | A custom writer instead of ClickHouse's Kafka table engine |
| [0012](0012-machine-info-through-kafka.md) | Machine info goes through Kafka, not straight from the collector to ClickHouse |
| [0013](0013-24-hour-retention.md) | 24 hours of samples and evaluations, no downsampling, machines kept forever |
| [0014](0014-one-second-checkpoints.md) | Flink checkpoints every second, untuned |

New record: copy the shape of an existing one (Status, Context, Decision, Consequences) and add a row above.
