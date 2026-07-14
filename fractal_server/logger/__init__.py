# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
This module provides logging utilities
"""

import logging
import logging.config
import sys
from pathlib import Path

import yaml

import fractal_server.logger._config_file_state as _state
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FORMATTER = logging.Formatter(LOG_FORMAT)


def get_logger(logger_name: str | None = None) -> logging.Logger:
    """
    Wrap the
    [`logging.getLogger`](https://docs.python.org/3/library/logging.html#logging.getLogger)
    function.

    The typical use case for this function is to retrieve a logger that was
    already defined, as in the following example:
    ```python
    def function1(logger_name):
        logger = get_logger(logger_name)
        logger.info("Info from function1")

    def funtion2():
        logger_name = "my_logger"
        logger = set_logger(logger_name)
        logger.info("Info from function2")
        function1(logger_name)
        close_logger(logger)
    ```

    Args:
        logger_name: Name of logger
    Returns:
        Logger with name `logger_name`
    """
    return logging.getLogger(logger_name)


def set_logger(
    logger_name: str,
    *,
    log_file_path: str | Path | None = None,
    default_logging_level: int | None = None,
) -> logging.Logger:
    """
    Set up a `fractal-server` logger

    The logger (a `logging.Logger` object) will have the following properties:

    * The attribute `Logger.propagate` set to `False`;
    * One and only one `logging.StreamHandler` handler, with severity level set
    to `FRACTAL_LOGGING_LEVEL` (or `default_logging_level`, if set), and
    formatter set as in the `logger.LOG_FORMAT`
    variable from the current module;
    * One or many `logging.FileHandler` handlers, including one pointint to
    `log_file_path` (if set); all these handlers have severity level set to
    `logging.DEBUG`.

    Note on external logging config (`FRACTAL_LOG_CONFIG_FILE`):
    When an external config is loaded (`_CONFIG_LOADED` is `True`),
    the `StreamHandler` setup is always skipped (the external config owns the
    logging hierarchy). However, if `log_file_path` is provided, the
    `FileHandler` is **still added** unconditionally. This is because certain
    log files (e.g. ``workflow.log``, task-collection logs) are functional
    artifacts that are read back into the database — they must always be
    written regardless of how application logging is configured.

    Args:
        logger_name: The identifier of the logger.
        log_file_path: Path to the log file.
        default_logging_level: Override for `settings.FRACTAL_LOGGING_LEVEL`

    Returns:
        logger: The logger, as configured by the arguments.
    """
    if _state._CONFIG_LOADED and log_file_path is None:
        return logging.getLogger(logger_name)

    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    current_stream_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    if not _state._CONFIG_LOADED and not current_stream_handlers:
        stream_handler = logging.StreamHandler()
        if default_logging_level is None:
            settings = Inject(get_settings)
            default_logging_level = settings.FRACTAL_LOGGING_LEVEL
        stream_handler.setLevel(default_logging_level)
        stream_handler.setFormatter(LOG_FORMATTER)
        logger.addHandler(stream_handler)

        # Emit once, on first setup: we could not log this earlier because
        # the logger was not yet available at the time of the failure.
        if _state._CONFIG_ERROR is not None:
            logger.warning(
                f"FRACTAL_LOG_CONFIG_FILE was set but failed to load "
                f"({_state._CONFIG_ERROR}). Falling back to built-in logging."
            )

    if log_file_path is not None:
        file_handler = logging.FileHandler(log_file_path, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(LOG_FORMATTER)
        logger.addHandler(file_handler)
        current_file_handlers = [
            handler
            for handler in logger.handlers
            if isinstance(handler, logging.FileHandler)
        ]
        if len(current_file_handlers) > 1:
            logger.warning(
                f"Logger {logger_name} has multiple file handlers: "
                f"{current_file_handlers}"
            )

    return logger


def close_logger(logger: logging.Logger) -> None:
    """
    Close all handlers associated to a `logging.Logger` object

    Args:
        logger: The actual logger
    """
    if _state._CONFIG_LOADED:
        # Only close FileHandlers; StreamHandlers are managed by the external
        # config and must not be touched.
        for handle in list(logger.handlers):
            if isinstance(handle, logging.FileHandler):
                handle.close()
        return
    for handle in logger.handlers:
        handle.close()


def reset_logger_handlers(logger: logging.Logger) -> None:
    """
    Close and remove all handlers associated to a `logging.Logger` object

    Args:
        logger: The actual logger
    """
    if _state._CONFIG_LOADED:
        # Only remove FileHandlers; StreamHandlers are managed by the external
        # config and must not be touched.
        for handle in list(logger.handlers):
            if isinstance(handle, logging.FileHandler):
                handle.close()
                logger.handlers.remove(handle)
        return
    close_logger(logger)
    logger.handlers.clear()


def config_uvicorn_loggers() -> None:
    """
    Change the formatter for the uvicorn access/error loggers.

    Skipped when an external logging config file is loaded, since that file
    already configures the uvicorn loggers.

    This is similar to https://stackoverflow.com/a/68864979/19085332. See also
    https://github.com/tiangolo/fastapi/issues/1508.

    This function is meant to work in two scenarios:

    1. The most relevant case is for a `gunicorn` startup command, with
       `--access-logfile` and `--error-logfile` options set.
    2. The case of `fractalctl start` (directly calling `uvicorn`).

    Because of the second use case, we need to check whether uvicorn loggers
    already have a handler. If not, we skip the formatting.
    """

    if _state._CONFIG_LOADED:
        return

    access_logger = logging.getLogger("uvicorn.access")
    if len(access_logger.handlers) > 0:
        access_logger.handlers[0].setFormatter(LOG_FORMATTER)

    error_logger = logging.getLogger("uvicorn.error")
    if len(error_logger.handlers) > 0:
        error_logger.handlers[0].setFormatter(LOG_FORMATTER)


def _load_logging_config(config_env: str) -> None:
    """
    Load logging configuration from a YAML file path.

    On success sets `_CONFIG_LOADED = True`. On failure sets
    `_CONFIG_ERROR` to the error message and prints a warning
    to stderr (because the application logger is not yet available at this
    point).
    """
    if _state._CONFIG_LOADED:
        return

    try:
        logging_config_path = Path(config_env)
        with logging_config_path.open("r") as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
        _state._CONFIG_LOADED = True
    except Exception as _e:
        _state._CONFIG_ERROR = str(_e)
        print(
            f"[fractal-server] WARNING: failed to load "
            f"LOG_CONFIG_FILE={config_env!r}: {_e}. "
            f"Falling back to built-in logging.",
            file=sys.stderr,
        )
