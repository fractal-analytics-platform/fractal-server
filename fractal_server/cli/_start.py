def start(
    *,
    host: str,
    port: int,
    reload: bool,
):
    import uvicorn

    uvicorn.run(
        "fractal_server.main:app",
        host=host,
        port=port,
        reload=reload,
    )
