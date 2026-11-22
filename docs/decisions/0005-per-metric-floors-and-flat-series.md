# 0005. Per-metric floors, and EWMA on a flat series

Status: Accepted

Source: commits f7ad2e7, a6e41f2, 7937ba1, 1e2a667; README.md (Scoring rules, MAD, EWMA).

## Context

A series that barely moves has a tiny spread, so every small step is flagged. Memory in use moving 32 KB on 470 MB scored 8 against a threshold of 3.5 (f7ad2e7). NAB values run from about 0.1 to millions, so no one floor in the values' units fits them all (1e2a667).

## Decision

The detector scale has a floor, `max(min_deviation, min_relative x level)`. A per-metric policy picks the detectors and floors: CPU 10 percentage points for both detectors (7937ba1), memory in use 2% of its level, any name starting with `nab.` no floors. Metrics outside the policy are not scored; the README gives `system.memory.limit` and `system.uptime` as examples that barely move or only grow. With both floors at 0 and a flat history, EWMA does not divide by a scale of 0: a flat continuation scores 0 and any rise scores infinity (1e2a667).

## Consequences

- Without the zero-scale fix EWMA raised `ZeroDivisionError` and failed the Flink task (README.md).
- MAD never flags at a scale of 0; EWMA can, because the current value is not yet in the variance.
- The floors are hand-tuned; the README has a query to count what gets through.
- The `nab.` check is a case-sensitive prefix match that includes the dot.
