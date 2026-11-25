# 0003. A window of the last 360 values, not a time window

Status: Accepted

## Context

Each detector scores the newest value of a series against the values before it. `MAD` needs 20 values and `EWMA` 11
(`min_points`). The window used to be the last hour of event time.

Most NAB files have one value every 5 minutes. An hour of that is 12 values, fewer than the 20 MAD needs, so MAD scored
none of a NAB replay. At the collector's default 1-second gap, an hour is about 3,600 values, more than the detectors need.

## Decision

- `AnomalyDetectionFunction` (`anomaly_detector/flink/processors/anomaly_detection_function.py`) keeps the last
  `WINDOW_POINTS = 360` values of each series, keyed by `(machine_id, name)`.
- The state is a Flink list state named `recent_values` holding plain doubles. Each record appends its value, the oldest
  drops once 360 are kept, and the value just appended is the one scored. The order is arrival order.
- There is no time cutoff. A value stays until newer ones push it out, including across a gap in the data.
- Event time (`timestamp * 1000`, 5 seconds of out-of-orderness in `EventTimeWatermarks`) still drives the watermark, but
  not the window.

| Sample rate | Time covered by 360 values |
|---|---|
| default collector gap, about 1 s | a little over 6 minutes |
| steady 10 s | 1 hour |
| 5 minutes (NAB) | 30 hours |

## Consequences

- The window is full at any sample rate, so every detector scores once 20 values have arrived.
- How much time the window covers depends on the sample rate. A baseline over 6 minutes of live data and one over 30 hours
  of NAB data are not the same kind of baseline.
- A pause is invisible to the detectors. After an outage, the first new value is compared with values from before it.
- The state of a series that stops reporting is never cleared: the list state has no TTL. Add a `StateTtlConfig` if
  machines come and go often.
- Checkpoints written before this change stored `(event time, value)` tuples under the same state name. Resume only from
  a checkpoint written with the list of doubles.
