#!/usr/bin/env bash
# Submits the anomaly detector, plus any extra jobs in /opt/flink/jobs, detached.
set -u

JOBMANAGER="jobmanager:8081"

until flink list -m "$JOBMANAGER" > /dev/null 2>&1; do
    echo "Waiting for Flink jobmanager at $JOBMANAGER..."
    sleep 2
done

# ponytail: all-or-nothing guard against duplicate submissions when this
# container is re-run against a cluster that already has jobs; match per job
# name if jobs ever need to be added to a running cluster one by one.
if ! flink list -r -m "$JOBMANAGER" | grep -q "No running jobs"; then
    echo "Cluster already has running jobs, skipping submission."
    exit 0
fi

# The anomaly detector lives in the mounted source tree. Extra scripts dropped
# in /opt/flink/jobs are submitted too. nullglob keeps an empty jobs directory
# from being passed to flink as a literal path.
shopt -s nullglob
jobs=(/opt/flink/src/activityreporter/anomaly_detector/main.py /opt/flink/jobs/*.py)
if [ ${#jobs[@]} -eq 0 ]; then
    echo "No PyFlink jobs found" >&2
    exit 1
fi

status=0
for job in "${jobs[@]}"; do
    echo "Submitting $job"
    # -pyfs puts src/ on PYTHONPATH for both this client and the taskmanager's
    # Python workers, so jobs can import the activityreporter package.
    if ! flink run -d -m "$JOBMANAGER" -pyfs /opt/flink/src -py "$job"; then
        echo "Failed to submit $job" >&2
        status=1
    fi
done
exit $status
