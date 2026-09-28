from dataclasses import dataclass
from datetime import datetime, timezone

import machineid
from fastapi import APIRouter

from activityreporter.agent.models import Machine, MachineFactory
from activityreporter.agent.service import MetricFactory

router = APIRouter(prefix="/v1")


@dataclass
class MetricObservation:
    name: str
    type: str
    unit: str
    value: float
    attributes: dict[str, str] | None = None


@dataclass
class MetricsResponse:
    machine_id: str
    timestamp: str
    metrics: list[MetricObservation]


@router.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/machine", tags=["machine"])
async def machine() -> Machine:
    return await MachineFactory.create_machine()


@router.get("/metrics", tags=["metrics"])
async def metrics() -> MetricsResponse:
    observations = await MetricFactory.create_metrics()
    return MetricsResponse(
        machine_id=machineid.id(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        metrics=[
            MetricObservation(name=m.name, type=m.type, unit=m.unit, value=m.value, attributes=m.attributes)
            for m in observations
        ],
    )
