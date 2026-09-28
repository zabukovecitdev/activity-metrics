import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from activityreporter.agent.api import router
from activityreporter.agent.discovery import ServiceAdvertiser

PORT = 8080


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with ServiceAdvertiser(PORT):
        yield


app = FastAPI(lifespan=lifespan)
app.include_router(router)


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
