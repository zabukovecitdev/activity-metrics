#!/usr/bin/env bash
# Submits every PyFlink job in /opt/flink/jobs to the session cluster, detached.
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

status=0
for job in /opt/flink/jobs/*.py; do
    echo "Submitting $job"
    # -pyfs puts src/ on PYTHONPATH for both this client and the taskmanager's
    # Python workers, so jobs can import shared code such as `core`.
    if ! flink run -d -m "$JOBMANAGER" -pyfs /opt/flink/src -py "$job"; then
        echo "Failed to submit $job" >&2
        status=1
    fi
done
exit $status
