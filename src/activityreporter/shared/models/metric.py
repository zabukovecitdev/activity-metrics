from dataclasses import dataclass


@dataclass
class Metric:
    """One metric sample.

    `metric_id` (a UUIDv7 string) identifies the sample from the moment the
    collector accepts it; the agent builds samples without one.
    """
    timestamp: float | int
    name: str
    type: str
    unit: str
    value: float
    machine_id: str
    metric_id: str | None = None
    attributes: dict[str, str] | None = None
