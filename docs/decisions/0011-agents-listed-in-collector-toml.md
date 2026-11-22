# 0011. Agents are listed in collector.toml, not discovered

Status: Accepted

Source: commits 31b4390 and 2468940, README.md (Agents), docs/architecture.md (agent row), docs/simplification-plan.md (question 11).

## Context

The collector first discovered agents over mDNS (2468940). Multicast does not cross the Docker bridge, so the collector ran on the host network, and on Docker Desktop that is the VM's network, so LAN discovery needed native Docker Engine (2468940).

## Decision

31b4390 replaced mDNS with a `collector.toml` list of agent base URLs, read once at startup. The collector refuses to start, naming the problem, when the file is missing, invalid TOML, or `agents` is not a non-empty list of http(s) URLs. The mDNS advertiser, browser and `zeroconf` dependency were removed, and the collector went back to the Compose network. The collector pulls from agents, so each agent only answers HTTP and knows nothing about Kafka (docs/architecture.md).

## Consequences

- Adding an agent means editing the file and `docker compose restart collector`.
- The collector must be able to reach every agent (plan, question 11).
- Agents have no authentication; this is on the "Not doing yet" list.
- Why discovery was dropped beyond the host-network cost: Not recorded.
