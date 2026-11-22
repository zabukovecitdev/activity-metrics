# 0003. Window of the last 360 values, not the last hour

Status: Accepted

Source: commit 1e2a667 (message), README.md and docs/architecture.md (Anomaly detection).

## Context

The window was the last hour of values per series. An hour of 5-minute data, which is most of the NAB benchmark, holds 12 values. MAD needs 20, so it scored none of that data.

## Decision

Per series, the job keeps the last 360 values, in arrival order, with no time cutoff. The list state `recent_values` holds plain doubles; the oldest drops once 360 are kept. 360 values is one hour at the 10-second scrape interval, the rate when this was chosen. Nothing restored the old state on deploy, so the state shape could change (1e2a667).

## Consequences

- The window stays full at any sample rate.
- At the collector's default 1-second gap a live series holds a little over six minutes; 5-minute data holds 30 hours (README.md).
- A pause leaves the values in place until newer ones push them out.
- Checkpoints written before the change stored (event time, value) tuples under the same name and must not be resumed (docs/operations.md).
