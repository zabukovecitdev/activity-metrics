#!/usr/bin/env bash
# The check functions are called through check(), which shellcheck can't follow.
# shellcheck disable=SC2329
# Checks a running stack (`make up`) end to end: containers, Kafka topics, the Flink job,
# ClickHouse tables, and Grafana. Needs an agent the collector can reach, e.g. `make agent`.
#
#   scripts/smoke-test.sh            check once
#   WAIT=300 scripts/smoke-test.sh   keep retrying failed checks for up to 300 seconds
set -u

WAIT="${WAIT:-0}"
CLICKHOUSE="http://localhost:8123/?user=user&password=password&database=metrics"
FLINK="http://localhost:8081"
GRAFANA="http://localhost:3000"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"
DASHBOARD_UID="ad8rn6h"
failures=0

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; [ -n "${2:-}" ] && printf '       %s\n' "$2"; failures=$((failures + 1)); }

# check "<description>" "<hint if it fails>" <command...>: retries until it succeeds or WAIT runs out.
check() {
    local description="$1" hint="$2"
    shift 2
    local deadline=$((SECONDS + WAIT))
    until "$@" > /dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            fail "$description" "$hint"
            return
        fi
        sleep 5
    done
    pass "$description"
}

clickhouse() { curl -sf "$CLICKHOUSE" --data-binary "$1"; }
positive() { [ "$("$@")" -gt 0 ] 2>/dev/null; }

service_running() { [ "$(docker compose ps --status running --services 2>/dev/null | grep -cx "$1")" -eq 1 ]; }
migrate_succeeded() { docker compose ps -a migrate --format '{{.State}} {{.ExitCode}}' | grep -q '^exited 0$'; }
topic_messages() {
    docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" \
        | awk -F: '{ sum += $3 } END { print sum + 0 }'
}
flink_job_running() {
    local overview
    overview="$(curl -sf "$FLINK/jobs/overview")" || return 1
    grep -q '"name":"Anomaly Detection"' <<< "$overview" && grep -q '"state":"RUNNING"' <<< "$overview"
}
recent_rows() { clickhouse "SELECT count() FROM $1 WHERE $2 > now() - INTERVAL 10 MINUTE"; }
evaluations_link_to_samples() {
    [ "$(clickhouse "SELECT count() FROM (SELECT metric_id FROM evaluations WHERE timestamp > now() - INTERVAL 10 MINUTE) AS e
                     LEFT ANTI JOIN metrics AS m USING (metric_id)")" -eq 0 ]
}
grafana_healthy() { curl -sf "$GRAFANA/api/health" | grep -q '"database": *"ok"'; }
grafana_datasource() { curl -sf -u "$GRAFANA_AUTH" "$GRAFANA/api/datasources/uid/clickhouse/health" | grep -qi '"status": *"ok"'; }
grafana_dashboard() { curl -sf -u "$GRAFANA_AUTH" "$GRAFANA/api/dashboards/uid/$DASHBOARD_UID" > /dev/null; }

echo "Containers"
for service in kafka clickhouse collector clickhouse-writer jobmanager taskmanager grafana; do
    check "$service is running" "docker compose logs $service" service_running "$service"
done
check "migrate exited 0" "docker compose logs migrate" migrate_succeeded

echo "Kafka"
check "raw_metrics has messages" "no agent reachable? run \`make agent\` or set COLLECTOR_ENDPOINTS" positive topic_messages raw_metrics
check "machines has messages" "docker compose logs collector | grep -i machine" positive topic_messages machines
check "evaluations has messages" "detectors need 11-20 samples per series (2-4 min); try WAIT=300" positive topic_messages evaluations

echo "Flink"
check "job 'Anomaly Detection' is RUNNING" "docker compose logs jobmanager; Web UI at $FLINK" flink_job_running

echo "ClickHouse"
check "metrics has rows from the last 10 minutes" "docker compose logs clickhouse-writer" positive recent_rows metrics timestamp
check "machines has a row seen in the last 10 minutes" "collector refreshes machine info every 5 minutes" positive recent_rows machines observed_at
check "evaluations has rows from the last 10 minutes" "see the Flink job and the evaluations topic above" positive recent_rows evaluations timestamp
check "every evaluation links to a stored sample" "metric_id mismatch between metrics and evaluations" evaluations_link_to_samples

echo "Grafana"
check "Grafana is healthy" "docker compose logs grafana" grafana_healthy
check "ClickHouse datasource works" "set GRAFANA_AUTH=user:password if you changed the admin password" grafana_datasource
check "dashboard '$DASHBOARD_UID' is provisioned" "docker/grafana/provisioning/dashboards" grafana_dashboard

if [ "$failures" -eq 0 ]; then
    echo "All checks passed."
else
    echo "$failures check(s) failed."
fi
exit $((failures > 0))
