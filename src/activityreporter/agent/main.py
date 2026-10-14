import asyncio

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from activityreporter.agent.api import v1_router
from activityreporter.shared.settings.agent_settings import AgentSettings

app = FastAPI()
app.include_router(v1_router.router)


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')


async def main() -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=AgentSettings().port)
    server = uvicorn.Server(config)
    await server.serve()


def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
