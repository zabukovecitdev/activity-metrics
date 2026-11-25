# 0007. `metric_id` (UUIDv7) as the identity of a sample

Status: Accepted

## Context

A sample is stored in `metrics`, and each detector's result for it in `evaluations`. The two have to be joined, and a
replayed copy of either has to collapse with the first one ([ADR 0004](0004-at-least-once-delivery.md)).

`(machine_id, name, timestamp)` is not unique. The agent stamps every metric of one response with the same timestamp, and
two series with the same name that differ only in `attributes` would share all three. Adding `attributes` to every key
and every join would work, but makes both wider.

## Decision

- `MetricsParser.parse` gives every `Metric` its own `metric_id`, `str(uuid.uuid7())`, when the collector accepts the
  agent's response. The agent does not send one.
- The id travels with the sample from then on: in the `raw_metrics` record, in `metrics.metric_id` (`UUID`), and in every
  evaluation of that sample. `Metric` and `Evaluation` reject a value that is not a UUID, and the writer skips such a
  record as malformed.
- `metric_id` is the last column of both sorting keys: `(machine_id, name, timestamp, metric_id)` for `metrics` and
  `(machine_id, metric_name, algorithm, timestamp, metric_id)` for `evaluations`. The series and time come first because
  queries filter on them.
- `evaluations` joins `metrics` on `metric_id` alone. The value is not repeated in `evaluations`.
- UUIDv7 starts with a millisecond timestamp, so ids sort roughly by the time the collector minted them.

## Consequences

- A Kafka redelivery or a writer replay carries the same id, so it collapses in ClickHouse.
- The id is assigned per collector. If two collectors scraped the same agent, the same reading would become two rows with
  different ids, where a key of `(machine_id, name, attributes, timestamp)` would have collapsed them. With one collector
  that does not happen. Running `make collector` while the Compose collector runs does it, so stop one first.
- A deterministic id, for example `uuid5` of `machine_id`, `name`, `attributes` and `timestamp`, would remove that
  caveat. It is not done, because one collector is the only setup this project runs.
- Records published before `metric_id` existed have none. The writer skips them.
