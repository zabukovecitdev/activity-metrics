from fastapi import APIRouter

from activityreporter.agent.describe_machine.machine_describer import MachineDescriber
from activityreporter.agent.sample_metrics.metrics_sampler import MetricsSampler
from activityreporter.contracts.machine import Machine
from activityreporter.contracts.metrics_response import MetricsResponse

router = APIRouter(prefix="/v1")
machine_describer = MachineDescriber()
metrics_sampler = MetricsSampler()


@router.get("/health", tags=["health"])
async def get_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/machine", tags=["machine"])
async def get_machine() -> Machine:
    return await machine_describer.describe()


@router.get("/metrics", tags=["metrics"])
async def get_metrics() -> MetricsResponse:
    return await metrics_sampler.sample()
