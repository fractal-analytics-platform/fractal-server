import json
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.jobs = {}
    yield
    with open("dump.txt", "r") as f:
        db = json.loads(f.read())
    for key in app.state.jobs.keys():
        db.update({key: "FAILED"})
    with open("dump.txt", "w") as f:
        f.write(json.dumps(db))


app = FastAPI(lifespan=lifespan)


@app.get("/status/")
async def get_status(request: Request):

    if not hasattr(request.app.state, "jobs"):
        jobs = {}
    else:
        jobs = request.app.state.jobs

    return {"jobs": jobs}


@app.post("/add-status/")
async def post_status(id: int, request: Request):

    if not hasattr(request.app.state, "jobs"):
        request.app.state.jobs = {}
        request.app.state.jobs[id] = "RUNNING"

        with open("dump.txt", "r") as f:
            db = json.loads(f.read())

        db.update({id: request.app.state.jobs[id]})

        with open("dump.txt", "w") as f:
            f.write(json.dumps(db))

    else:
        request.app.state.jobs[id] = "RUNNING"

        with open("dump.txt", "r") as f:
            db = json.loads(f.read())
        print(db)

        db.update({id: request.app.state.jobs[id]})

        with open("dump.txt", "w") as f:
            f.write(json.dumps(db))

    return {"jobs": app.state.jobs}
