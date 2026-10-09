from datetime import datetime, timezone

from fastapi import APIRouter

from activityreporter.agent import service
from activityreporter.shared.models import Machine, MetricObservation, MetricsResponse

router = APIRouter(prefix="/v1")


@router.get("/health", tags=["health"])
async def get_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/machine", tags=["machine"])
async def get_machine() -> Machine:
    return await service.describe_machine()


@router.get("/metrics", tags=["metrics"])
async def get_metrics() -> MetricsResponse:
    metrics = await service.collect_metrics()
    return MetricsResponse(
        machine_id=metrics[0].machine_id,
        timestamp=datetime.fromtimestamp(metrics[0].timestamp, timezone.utc),
        metrics=[
            MetricObservation(name=m.name, type=m.type, unit=m.unit, value=m.value, attributes=m.attributes)
            for m in metrics
        ],
    )
