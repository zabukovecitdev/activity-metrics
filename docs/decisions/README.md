# Decisions

Why things are the way they are. Each record cites its source (a file or commit) and says "Not recorded" where the reason was never written down. Format: MADR-lite.

| # | Title | Summary |
|---|---|---|
| [0001](0001-kafka-between-every-stage.md) | Kafka between every stage | Kafka topics carry every hand-off between collector, scorer and writer. |
| [0002](0002-flink-scores-the-samples.md) | Flink scores the samples | A PyFlink job keeps per-series state and scores; a plain consumer would do. |
| [0003](0003-count-based-window-of-360-values.md) | Window of the last 360 values, not the last hour | State is the last 360 values per series, not the last hour. |
| [0004](0004-mad-and-ewma-one-evaluation-each.md) | MAD and EWMA, one evaluation per sample per algorithm | Every scored sample gets one evaluation per detector, anomalous or not. |
| [0005](0005-per-metric-floors-and-flat-series.md) | Per-metric floors, and EWMA on a flat series | Per-metric floors stop tiny steps being flagged; EWMA scores flat series without dividing by 0. |
| [0006](0006-metric-id-uuidv7-assigned-by-collector.md) | metric_id is a UUIDv7 assigned by the collector | The collector gives each sample a UUIDv7 id, used as the join key. |
| [0007](0007-idempotent-writes-replacing-merge-tree.md) | Safe replays: ReplacingMergeTree and commit after write | Offsets are committed after the insert; ReplacingMergeTree collapses replays. |
| [0008](0008-daily-partitions-and-24-hour-ttl.md) | Daily partitions and 24-hour retention | Daily partitions avoid part explosion on replay; 24-hour TTL. |
| [0009](0009-dead-letter-topics-for-invalid-responses.md) | Dead-letter topics for invalid agent responses | Invalid agent responses go to <topic>_dlq; the writer only skips and logs. |
| [0010](0010-one-sink-per-topic-on-its-own-thread.md) | One sink per topic, each on its own thread | One consumer, client and thread per topic; one failure stops all. |
| [0011](0011-agents-listed-in-collector-toml.md) | Agents are listed in collector.toml, not discovered | Agents come from collector.toml, read once; mDNS removed. |
| [0012](0012-flink-application-mode-and-at-least-once-sink.md) | Flink Application Mode, 1-second checkpoints, at-least-once sink | The jobmanager runs the job; 1 s checkpoints; at-least-once Kafka sink. |
| [0013](0013-layout-by-capability-enforced-and-generated-docs.md) | Layout by capability, enforced by import-linter, behaviour catalogue from tests | Capability folders, adapters/, contracts/, import-linter, behaviour.md from test names. |
