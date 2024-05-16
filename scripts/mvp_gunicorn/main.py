from fastapi import FastAPI

app = FastAPI()
app.state.jobs = []


@app.get("/status/")
async def get_status():
    return {"jobs": app.state.jobs}


@app.post("/add-status/")
async def post_status(id: int):
    app.state.jobs.append(id)
    return {"jobs": app.state.jobs}
