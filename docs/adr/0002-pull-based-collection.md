# 0002. Pull-based collection

Status: Accepted

## Context

Every watched machine runs an agent. Something has to move its samples into Kafka. Either the agent pushes them, which
puts a Kafka client, the broker address and retry logic on every machine, or a central process pulls them.

The first version found agents over mDNS (`_activityrep._tcp`). Multicast does not cross the Docker bridge, so the
collector had to run on the host network, and on Docker Desktop that is the VM's network, not the LAN. Commit `31b4390`
replaced discovery with a list in `collector.toml`.

## Decision

- **Agents only answer HTTP.** The FastAPI app (`agent/main.py`, `agent/api/v1_router.py`) serves `/v1/metrics`,
  `/v1/machine` and `/v1/health`. It knows nothing about Kafka, the database or the other agents.
- **The collector scrapes them.** `AgentsFileLoader` reads the base URLs from `collector.toml` once at startup and
  refuses to start on a missing file or an invalid list. `AgentUrls` adds `/v1/metrics` and `/v1/machine`.
- **One round, then a gap.** `Collector.run` scrapes every agent concurrently (`asyncio.gather`), then sleeps
  `COLLECTOR_SCRAPE_INTERVAL_SECONDS` (default 1). The gap is after the round, so the cadence is the setting plus the
  time the round took. `HttpAgentClient` gives each request 2 seconds to connect and 2 to read.
- **Machine info is pulled less often.** `Collector._refresh_machine` fetches `/v1/machine` on the first scrape of an
  endpoint and every `COLLECTOR_MACHINE_REFRESH_SECONDS` (default 300) after a successful publish. A failure is retried
  on the next scrape.
- **The collector owns validation.** `Collector._receive` validates both responses and sends an invalid body, as
  received, to `raw_metrics_dlq` or `machines_dlq`.

## Consequences

- The collector must reach every agent over the network. An agent behind NAT or a firewall cannot be watched without
  opening its port.
- One collector is a single point of failure. A second one would store every sample twice
  ([ADR 0007](0007-metric-id-as-sample-identity.md)).
- The collector controls the rate. A slow agent delays the end of its round by at most the 2-second timeouts, and a
  failing one is logged and skipped without cancelling the others.
- The agent has no authentication and listens on `0.0.0.0`. Anyone who can reach port 8080 can read its metrics.
- `MetricsService.collect_metrics` calls `psutil.cpu_percent(interval=0.1)` inside an `async def`, so each `/v1/metrics`
  request blocks the agent's event loop for about 100 ms.
