from dataclasses import dataclass


@dataclass
class Machine:
    machine_id: str
    hostname: str
    os: str
    os_version: str
    architecture: str
    cores: int
    total_disk_memory: float
    total_memory: float
    uptime: float
    last_boot: str
