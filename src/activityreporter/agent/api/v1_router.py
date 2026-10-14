from fastapi import APIRouter

from activityreporter.agent.services.metrics_service import MetricsService
from activityreporter.shared.models.machine import Machine
from activityreporter.shared.models.metrics_response import MetricsResponse

router = APIRouter(prefix="/v1")
service = MetricsService()


@router.get("/health", tags=["health"])
async def get_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/machine", tags=["machine"])
async def get_machine() -> Machine:
    return await service.describe_machine()


@router.get("/metrics", tags=["metrics"])
async def get_metrics() -> MetricsResponse:
    return await service.collect_metrics()
