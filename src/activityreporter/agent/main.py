import asyncio
import os

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from activityreporter.agent import api

PORT = int(os.environ.get("PORT", "8080"))

app = FastAPI()
app.include_router(api.router)


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
