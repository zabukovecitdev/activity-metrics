from fastapi import APIRouter

from client.machine_info import Machine, MachineFactory
from client.reporter import HttpReporter
from core.metrics import Metric

router = APIRouter(prefix="/v1")


@router.get("/metrics/", tags=["metrics"])
async def metrics() -> list[Metric]:
    return await HttpReporter().get()


@router.get("/machine", tags=["machine"])
async def machine() -> Machine:
    return await MachineFactory.create_machine()
