import logging

import fractal_server.logger._config_file_state as _logger_module
from fractal_server.logger import config_uvicorn_loggers


def test_config_uvicorn_loggers():
    """
    This test simply runs `config_uvicorn_loggers`, but it does not assert
    anything. It is only meant to catch some trivial errors.
    """
    config_uvicorn_loggers()


def test_config_uvicorn_loggers_is_noop_when_external_config_loaded():
    """
    When _EXTERNAL_CONFIG_LOADED is True, config_uvicorn_loggers() must not
    overwrite the formatter already set on the uvicorn.access handler.
    """
    original_flag = _logger_module._CONFIG_LOADED
    access_logger = logging.getLogger("uvicorn.access")
    added_handler = False
    try:
        if not access_logger.handlers:
            access_logger.addHandler(logging.StreamHandler())
            added_handler = True

        sentinel_formatter = logging.Formatter("%(message)s [SENTINEL]")
        access_logger.handlers[0].setFormatter(sentinel_formatter)

        _logger_module._CONFIG_LOADED = True
        config_uvicorn_loggers()

        assert access_logger.handlers[0].formatter is sentinel_formatter
    finally:
        _logger_module._CONFIG_LOADED = original_flag
        if added_handler:
            access_logger.handlers.clear()
