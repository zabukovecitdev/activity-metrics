# 0006. Python 3.10 in the Flink image, 3.14 everywhere else

Status: Accepted

## Context

The package requires Python 3.14 (`requires-python = ">=3.14"`). The collector mints ids with `uuid.uuid7()`, which is
new in 3.14, and uses PEP 695 type parameters (`Collector._receive[T]`).

PyFlink 1.19 supports CPython 3.8 to 3.11, and the upstream image `flink:1.19.1-scala_2.12-java11` ships no Python. The
`apache-flink` wheel also cannot be installed in the `uv` environment: its `pemja` dependency needs a JDK to build.

## Decision

- **Two images.** The root `Dockerfile` builds `activity-metrics` on `python:3.14-slim` and installs the locked versions
  with `uv sync --frozen --no-default-groups`. It runs the agent, the collector and `clickhouse-writer`.
  `docker/flink/Dockerfile` builds `activity-metrics-flink` from the Flink image and installs the distribution's
  `python3` with apt, which is Python 3.10, then the packages in `docker/flink/requirements.txt` with pip.
- **The Flink job's packages are pinned.** `docker/flink/requirements.txt` pins `apache-flink==1.19.1`, the Flink
  version of the base image, and `pydantic-settings==2.15.0`, `pydantic==2.13.5` and `pydantic-core==2.46.5`, the
  versions in `uv.lock`. All three support Python 3.10, and `pydantic-core` has `cp310` wheels. The image and
  `make anomaly-detector` install from that file. A change to these packages in `uv.lock` is copied there by hand.
- **The job runs from the source tree.** The jobmanager mounts `./src` read-only and starts
  `activity_metrics.anomaly_detector.main` with `-pyfs`, so the package is not installed in the Flink image.
  `env.set_python_executable(sys.executable)` makes the Python workers use the same interpreter.
- **3.10 code is checked as 3.10.** Code under `src/activity_metrics/anomaly_detector/` and
  `src/activity_metrics/shared/` runs in the Flink image. A `ruff.toml` in each directory sets `target-version = "py310"`,
  and pyright's `executionEnvironments` in `pyproject.toml` set `pythonVersion = "3.10"` for both. Newer syntax fails
  `make check` and CI instead of the Flink job.
- **PyFlink stays at the edge.** Only `anomaly_detector/flink/` and `anomaly_detector/main.py` import PyFlink. The
  detectors, `MetricEvaluator` and the shared models do not, so their tests run on 3.14 in the `uv` environment. Pyright
  does not report the missing `pyflink` imports under `anomaly_detector/`.

## Consequences

- `shared/` cannot use `typing.Self`, `datetime.UTC`, PEP 695 generics or other syntax newer than 3.10, even though the
  rest of the package can.
- The shared models run on the same pydantic versions in the job as in the tests, as long as
  `docker/flink/requirements.txt` follows `uv.lock`. Nothing checks that it does. The other dependencies of those
  packages, such as `typing-extensions` and `python-dotenv`, are not pinned in the image.
- The process function, the source, the sink and the type information under `anomaly_detector/flink/` have no tests.
  `make smoke` against a running stack is the only check. CI runs it in `.github/workflows/smoke.yml`.
- `make anomaly-detector` builds its local venv with `python3.10`, the version of the image, so it needs a `python3.10`
  on `PATH`.
- Moving the job to a newer Python means a newer PyFlink, which means a newer Flink image and Kafka connector.
