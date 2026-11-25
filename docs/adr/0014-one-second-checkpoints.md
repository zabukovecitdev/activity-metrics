# 0014. Flink checkpoints every second, untuned

Status: Accepted

## Context

`anomaly_detector/main.py` calls `env.enable_checkpointing(1000)` in `EXACTLY_ONCE` mode, with a 500 ms minimum pause
between checkpoints and a 60-second timeout. Compose sets `state.checkpoints.dir` to `file:///checkpoints` on the
`flink_checkpoints` volume and retains checkpoints on cancellation.

The interval came in with the change that made checkpoints durable (#22, squashed into commit `31b4390`), whose message
says only "Enable exactly-once checkpoints every second". Nothing in the repository measured it or compared it with
another value. It was never tuned.

A checkpoint here covers three things: the Kafka source's offsets, the `recent_values` list state of
`AnomalyDetectionFunction`, at most 360 doubles (about 3 KB) per scored series, and a flush of the Kafka sink's
producer. No state backend is configured, so Flink uses its default heap backend and writes a full snapshot each time.

A checkpoint only helps a failure inside a running job. `docker compose restart jobmanager` starts a new job with no
savepoint, empty windows, and `raw_metrics` read from `earliest` again ([ADR 0004](0004-at-least-once-delivery.md)).

## Decision

Keep 1 second for now and record it as untuned. The trade-off is the replay window against the checkpoint overhead:

- **Replay window.** After a task failure the job restores the last completed checkpoint and reads `raw_metrics` again
  from its offsets. What repeats is at most the interval plus the time the checkpoint took. At 1 second that is about
  one collector round. At 30 seconds it would be about 30 rounds, four evaluations per agent each. The repeats have the
  same sorting key as the first copy and collapse in `ReplacingMergeTree`, so a longer window costs duplicate rows until
  a merge, not wrong data.
- **Overhead.** 86,400 checkpoints a day. Each one sends a barrier through the source, the Python operator and the sink,
  makes the Python worker finish its current bundle, flushes the Kafka producer, writes a new checkpoint directory to
  the volume and commits the source offsets to the `anomaly-detector` group. With a few machines the state is a few KB,
  so the cost is mostly this fixed work, not bytes. It has not been measured. The *Checkpoints* tab of the Flink Web UI
  shows the duration and size of each one.

## Consequences

- Evaluations do not wait for a checkpoint to reach Kafka, because the sink is `AT_LEAST_ONCE`, not transactional. A
  longer interval would not delay the dashboard.
- The committed offsets of `anomaly-detector` are at most about a second behind, so its consumer lag is accurate. The
  job does not read them: it starts from `earliest`.
- The interval should go up, to 10 to 30 seconds, if the checkpoint duration in the Web UI approaches 1 second, for
  example with many more series. It should stay short if the sink moves to `EXACTLY_ONCE`, because a transactional sink
  only makes evaluations visible to `read_committed` readers when a checkpoint completes, so the interval becomes the
  latency.
- If restarts start from a savepoint or the latest retained checkpoint, the interval also bounds how much a restart
  repeats, not only a task failure.
