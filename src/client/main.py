from fastapi import FastAPI
from starlette.responses import RedirectResponse

from client.metric_factory import Metrics
from client.reporter import HttpReporter

app = FastAPI()

@app.get("/metrics/", tags=["metrics"])
async def metrics() -> Metrics:
    return await HttpReporter().get()


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')
