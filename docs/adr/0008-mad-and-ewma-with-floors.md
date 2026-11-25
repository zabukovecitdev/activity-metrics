# 0008. MAD and EWMA, with absolute and relative floors

Status: Accepted

## Context

There is no labelled data for these machines, so the detectors have to be simple enough to reason about and tune by
looking at the dashboard. Two failure modes showed up early:

- A series that barely moves has a tiny spread, so every small step scores far above the threshold. Memory in use
  changing by 32 KB on a 470 MB median scored 8 with MAD, against a threshold of 3.5 (commit `f7ad2e7`).
- A floor in the values' units means different things per metric. EWMA's `MIN_DEVIATION` of 10 was 10 percentage points
  for CPU and 10 bytes for memory (commit `a6e41f2`).

## Decision

Two detectors, in `anomaly_detector/algorithms/`, both implementing `Detector` (`name`, `min_points`, `evaluate`):

- **`MAD`**: the modified z-score of the newest value against the window median, `|last - median| / (1.4826 × MAD)`,
  flagged at `>= 3.5`, in both directions. When over half the values equal the median, MAD is 0 and `1.253314 × mean
  absolute deviation` stands in (Iglewicz and Hoaglin). Needs 20 values.
- **`EWMA`**: an exponentially weighted average and variance with `alpha = 0.1`, seeded with the first value. Each value
  is scored against the average and variance from before it. Only rises are flagged, when the deviation divided by the
  scale is `> 1`. After a 10-reading warm-up, a flagged value moves the average with `alpha × 0.1` (`ANOMALY_DAMPING`), so
  a spike does not drag the baseline along. Needs 11 values.

They fail differently. MAD is robust to outliers in the window and sees drops. EWMA follows a drifting level and reacts
to the newest values first. The dashboard draws both, so the difference is visible.

Each detector's scale has a floor, the larger of two values:

- `min_deviation`, in the values' units;
- `min_relative`, a fraction of the level (the median for MAD, the average for EWMA).

MAD floors the scale at `max(min_deviation, min_relative × |median|) / THRESHOLD`, so a smaller deviation never reaches
the threshold. EWMA's scale is `max(3 × standard deviation, min_deviation, min_relative × |average|)`.

Per metric, in `MetricEvaluator.DETECTORS` (`anomaly_detector/services/metric_evaluator.py`):

| Metric | MAD | EWMA |
|---|---|---|
| `system.cpu.utilization` | `min_deviation=10.0` | `min_deviation=10.0` |
| `system.memory.usage` | `min_relative=0.02` | `min_deviation=0.0, min_relative=0.02` |
| `nab.<file name>` | no floor | `min_deviation=0.0`, no floor |

- CPU is in percentage points, so 10 is an absolute floor.
- Memory in use sits near one level and drifts, so only a change of 2% of that level counts. EWMA clears its class
  default `min_deviation` of 10, which would be 10 bytes.
- NAB values range from about 0.1 to millions, so no floor in the values' units fits them all. A name that starts with
  `nab.` (`NAB_PREFIX`, case-sensitive, dot included) uses `NAB_DETECTORS` without floors.
- Every other metric is not scored.

**A scale of 0.** The scale is 0 when the window has no spread and both floors are 0. A relative floor is also 0 when
the level is 0, so a flat series of zeros with only `min_relative` set gets there too.

- MAD never flags it. A MAD scale of 0 means every value in the window is equal, the newest one included.
- EWMA does not divide by it. The newest value is not in the variance yet, so a rise is still visible: a flat
  continuation scores 0, a drop scores 0, and any rise scores infinity and is flagged. `upper` equals the average. A
  thirty-value window of `5.0` has `baseline` 5 and `upper` 5; appending `5.1` is flagged, appending `4.9` is not. Before
  commit `1e2a667` the division raised `ZeroDivisionError` and failed the Flink task.

## Consequences

- The floors are tuned by eye on these machines. There is no precision or recall measurement yet. The NAB replay is the
  planned way to get one.
- The score and threshold stay inside the detector. Only `baseline`, `lower`, `upper` and `is_anomaly` are published
  ([ADR 0009](0009-an-evaluation-for-every-scored-sample.md)).
- A floorless EWMA on a flat `nab.` file flags every rise, however small, and draws its upper band on the baseline.
- To score another metric, add its entry to `MetricEvaluator.DETECTORS`. To add an algorithm, write a class with the
  `Detector` shape and add it to the lists. Neither needs a schema or dashboard change.
