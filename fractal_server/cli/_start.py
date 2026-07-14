import uvicorn


def start(
    *,
    host: str,
    port: int,
    reload: bool,
):
    uvicorn.run(
        "fractal_server.main:app",
        host=host,
        port=port,
        reload=reload,
    )
