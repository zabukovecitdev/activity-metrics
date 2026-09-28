#!/usr/bin/env bash
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

exec flink run -d -m "$JOBMANAGER" -pyfs /opt/flink/src -pym activityreporter.anomaly_detector.main
