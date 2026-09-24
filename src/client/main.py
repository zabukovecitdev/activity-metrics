import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from client.discovery import ServiceAdvertiser
from client.machine_info import MachineInfo, MachineInfoFactory
from client.reporter import HttpReporter
from core.metrics import RawMetrics

PORT = 8080


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with ServiceAdvertiser(PORT):
        yield


app = FastAPI(lifespan=lifespan)

@app.get("/metrics/", tags=["metrics"])
async def metrics() -> RawMetrics:
    return await HttpReporter().get()


@app.get("/info/", tags=["info"])
async def info() -> MachineInfo:
    return await MachineInfoFactory.create_machine_info()


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')


async def main() -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT)
    server = uvicorn.Server(config)
    await server.serve()

def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
