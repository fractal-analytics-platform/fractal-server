from fractal_server.main import config_uvicorn_loggers


def test_config_uvicorn_loggers():
    """
    This test simply runs `config_uvicorn_loggers`, but it does not assert
    anything. It is only meant to catch some trivial errors.
    """
    config_uvicorn_loggers()
