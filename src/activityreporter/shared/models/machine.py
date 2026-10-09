from pydantic import AwareDatetime, BaseModel, Field, NonNegativeInt


class Machine(BaseModel):
    """What the agent serves on /v1/machine and the collector publishes on the machines topic.

    Only what changes rarely; uptime is the `system.uptime` metric instead.
    `last_boot` travels as ISO-8601 with an offset, `observed_at` as epoch
    seconds of when the agent described the machine. Unknown fields are
    ignored, so a producer may add one before every consumer knows it.
    """
    machine_id: str = Field(min_length=1)
    hostname: str
    os: str
    os_version: str
    architecture: str
    cores: NonNegativeInt
    total_memory: NonNegativeInt
    total_disk: NonNegativeInt
    last_boot: AwareDatetime
    observed_at: float
