from fastapi import FastAPI
from fastapi import Request

app = FastAPI()


@app.get("/status/")
async def get_status(request: Request):

    if not hasattr(request.app.state, "jobs"):
        jobs = []
    else:
        jobs = request.app.state.jobs

    return {"jobs": jobs}


@app.post("/add-status/")
async def post_status(id: int, request: Request):

    if not hasattr(request.app.state, "jobs"):
        request.app.state.jobs = []
        app.state.jobs.append(id)
    else:
        app.state.jobs.append(id)

    return {"jobs": app.state.jobs}
