from dataclasses import dataclass


@dataclass
class Machine:
    """What the agent serves on /v1/machine and the collector publishes on the machines topic.

    Only what changes rarely; uptime is the `system.uptime` metric instead.
    `last_boot` is UTC ISO-8601, `observed_at` epoch seconds of when the agent
    described the machine.
    """
    machine_id: str
    hostname: str
    os: str
    os_version: str
    architecture: str
    cores: int
    total_memory: int
    total_disk: int
    last_boot: str
    observed_at: float
