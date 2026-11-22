# 0013. Layout by capability, enforced by import-linter, behaviour catalogue from tests

Status: Accepted

Source: commit 312b421 (previous layout), docs/simplification-plan.md (Phase 4), this restructure (2026-11-22).

## Context

The previous layout grouped files by role: `services/`, `models/`, `parsers/`, `clients/`, `producers/`, `consumers/`, `repositories/`, `sinks/`, plus `shared/` (312b421). A new reader could not tell what the system does from the folder names. docs/simplification-plan.md (Phase 4) notes that the README had grown to three jobs and that someone landing on the repo should understand it in two minutes.

## Decision

Under each app, top-level folders are capabilities named as what they do (`scrape_agents/`, `split_snapshots/`, `score_samples/`, `load_topics_into_tables/`). I/O lives in `adapters/`. What crosses a process boundary lives in `contracts/`. One class per file, the file named after the class, empty `__init__.py`, `main.py` only wires. import-linter (`make lint`) enforces: the apps share code only through `contracts/`; `contracts/` depends on no app; only `anomaly_detector/flink/` and `main.py` import PyFlink; business folders do not import `kafka`, `clickhouse_connect`, `httpx` or `pyflink`. Tests mirror `src/`, with names that are sentences of behaviour. `make docs` generates docs/behaviour.md from them, and a test fails when it is stale.

## Consequences

- The architecture rules are checked, not only described.
- Changing a behaviour means renaming a test, or the stale-docs test fails.
- Env variable prefixes, consumer group ids and table names are not part of this move (the same held in 312b421).
- Alternatives weighed: Not recorded.
