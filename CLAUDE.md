# Code complexity

Keep functions simple. Both checks are configured in `pyproject.toml`:

- Cognitive complexity (complexipy), max 15 per function: `uv run complexipy`
- Cyclomatic complexity (ruff C901 / mccabe), max 10: `uv run ruff check`

When writing or changing code, run both on the touched files and keep every function under the limits.
Aim well below them (cognitive ≤ 8 is a good target). Reduce complexity with early returns, small
helper functions and flatter control flow. Do not use `# noqa` or `# complexipy: ignore` to silence them.
Show the worst offenders with `uv run complexipy --sort desc --top 10`.

# Types

`uv run pyright` must pass with no errors. Fix the type, do not add `# type: ignore` or `# pyright: ignore`.
Code under `src/activity_metrics/anomaly_detector/` and `src/activity_metrics/shared/` runs in the Flink image
on Python 3.10. Ruff (`ruff.toml` in each of those directories) and pyright (`executionEnvironments`) check it
as 3.10, so newer syntax fails `make check`.

`make check` runs everything CI runs: ruff, the format check, pyright, complexipy and the tests.
