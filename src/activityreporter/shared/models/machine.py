from pydantic import AwareDatetime, BaseModel, Field, NonNegativeInt


class Machine(BaseModel):
    """What the agent serves on /v1/machine and the collector publishes on the machines topic.

    Only what changes rarely; uptime is the `system.uptime` metric instead.
    `last_boot` travels as ISO-8601 with an offset, `last_seen_at` as epoch
    seconds of when the agent described the machine. Unknown fields are
    ignored, so a producer may add one before every consumer knows it.
    """
    machine_id: str = Field(min_length=1)
    hostname: str = Field(min_length=1)
    os: str = Field(min_length=1)
    os_version: str = Field(min_length=1)
    architecture: str = Field(min_length=1)
    cores: NonNegativeInt
    total_memory: NonNegativeInt
    total_disk: NonNegativeInt
    last_boot: AwareDatetime
    last_seen_at: float
