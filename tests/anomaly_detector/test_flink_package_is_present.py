import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# main.py imports these. The repo-root `/flink/` venv ignore must not hide the package.
JOB_MODULES = (
    "src/activityreporter/anomaly_detector/flink/sources/kafka_metrics_source.py",
    "src/activityreporter/anomaly_detector/flink/sinks/kafka_evaluations_sink.py",
    "src/activityreporter/anomaly_detector/flink/type_info/evaluation_type_info.py",
    "src/activityreporter/anomaly_detector/flink/watermarks/event_time_watermarks.py",
)


def test_flink_job_modules_exist_and_are_not_gitignored():
    for rel in JOB_MODULES:
        assert (ROOT / rel).is_file(), rel
        ignored = subprocess.run(["git", "check-ignore", "--no-index", "-q", rel], cwd=ROOT)
        assert ignored.returncode == 1, rel
