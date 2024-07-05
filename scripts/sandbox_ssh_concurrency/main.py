from contextlib import asynccontextmanager

from fabric import Connection
from fastapi import FastAPI
from fastapi import Request

from fractal_server.ssh._fabric import check_connection
from fractal_server.ssh._fabric import run_command_over_ssh


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Setup START")
    app.state.connection = Connection(
        host="172.20.0.2",
        user="fractal",
        connect_kwargs={"password": "fractal"},
    )
    check_connection(app.state.connection)
    print(f"Startup OK, {app.state.connection.is_connected=}")

    yield

    print("Shutdown START")
    app.state.connection.close()
    print(f"Shutdown OK, {app.state.connection.is_connected=}")


app = FastAPI(lifespan=lifespan)


@app.get("/ssh")
async def ssh_endpoint(request: Request):
    run_command_over_ssh(
        cmd="sleep 10", connection=request.app.state.connection
    )


@app.get("/alive")
async def alive():
    print("Alive!")
    return "ALIVE"
