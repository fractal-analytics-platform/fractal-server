import json
import os
import signal
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request

file_path = "dump.txt"


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("STARTUP")
    if not os.path.isfile(file_path):
        with open(file_path, "w") as f:
            db = f.write(json.dumps({}))
    app.state.jobs = {}
    yield
    print("SHUTDOWN")
    with open(file_path, "r") as f:
        db = json.loads(f.read())
    for key in app.state.jobs.keys():
        db.update({key: "FAILED"})
    with open(file_path, "w") as f:
        f.write(json.dumps(db))


app = FastAPI(lifespan=lifespan)


@app.get("/status/")
async def get_status(request: Request):

    jobs = request.app.state.jobs

    return {"jobs": jobs}


@app.post("/add-status/")
async def post_status(id: int, request: Request):

    request.app.state.jobs[id] = "RUNNING"

    with open(file_path, "r") as f:
        db = json.loads(f.read())
    print(db)

    db.update({id: request.app.state.jobs[id]})

    with open(file_path, "w") as f:
        f.write(json.dumps(db))

    return {"jobs": app.state.jobs}


# SIGABORT HANDLING
def handle_timeout(signum, frame):
    print(f"RECEIVERD SIGNAL {signum}")
    os.kill(os.getpid(), signal.SIGTERM)


signal.signal(signal.SIGABRT, handle_timeout)
