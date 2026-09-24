import asyncio

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from client.machine_info import MachineInfo, MachineInfoFactory
from client.metric_factory import Metrics
from client.reporter import HttpReporter

app = FastAPI()

@app.get("/metrics/", tags=["metrics"])
async def metrics() -> Metrics:
    return await HttpReporter().get()


@app.get("/info/", tags=["info"])
async def info() -> MachineInfo:
    return await MachineInfoFactory.create_machine_info()


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')


async def main() -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=8080)
    server = uvicorn.Server(config)
    await server.serve()

def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
