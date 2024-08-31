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
import datetime
import logging
import time
from pathlib import Path
from typing import Optional
from typing import Union
from zoneinfo import ZoneInfo

from .config import get_settings
from .syringe import Inject

settings = Inject(get_settings)


def _converter(timestamp: float) -> time.struct_time:
    return (
        datetime.datetime.fromtimestamp(timestamp)
        .astimezone(ZoneInfo(settings.FRACTAL_LOGGING_TIMEZONE))
        .timetuple()
    )


class FractalLoggingFormatter(logging.Formatter):
    def converter(self, timestamp: float) -> time.struct_time:
        return _converter(timestamp)


def _utc_offset(timezone: str) -> str:
    """
    Input strings must come from `zoneinfo.available_timezones()`,
    otherwise raises a `ZoneInfoNotFoundError`.

    Examples input -> output:
    * "Canada/Newfoundland" -> "UTC-2:30"
    * "Europe/Rome" -> "UTC+2"
    * "UTC" -> "UTC+0"

    """
    timedelta = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone(ZoneInfo(timezone))
        .utcoffset()
    )
    if timedelta.days < 0:
        sign = "-"
        seconds = 24 * (60**2) - timedelta.seconds
    else:
        sign = "+"
        seconds = timedelta.seconds

    hours = seconds // (60**2)
    minutes = (seconds % (60**2)) // 60
    minutes_string = f":{minutes:02}" if minutes > 0 else ""

    return f"UTC{sign}{hours}{minutes_string}"


LOG_FORMAT = (
    "%(asctime)s::%(msecs)03d - %(name)s - %(levelname)s - %(message)s"
)
DATE_FORMAT = (
    f"%Y-%m-%d %H:%M:%S({_utc_offset(settings.FRACTAL_LOGGING_TIMEZONE)})"
)
LOG_FORMATTER = FractalLoggingFormatter(LOG_FORMAT, DATE_FORMAT)


def get_logger(logger_name: Optional[str] = None) -> logging.Logger:
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

    Arguments:
        logger_name: Name of logger
    Returns:
        Logger with name `logger_name`
    """
    return logging.getLogger(logger_name)


def set_logger(
    logger_name: str,
    *,
    log_file_path: Optional[Union[str, Path]] = None,
) -> logging.Logger:
    """
    Set up a `fractal-server` logger

    The logger (a `logging.Logger` object) will have the following properties:

    * The attribute `Logger.propagate` set to `False`;
    * One and only one `logging.StreamHandler` handler, with severity level set
    to
    [`FRACTAL_LOGGING_LEVEL`](../../../../configuration/#fractal_server.config.Settings.FRACTAL_LOGGING_LEVEL)
    and formatter set as in the `logger.LOG_FORMAT` variable from the current
    module;
    * One or many `logging.FileHandler` handlers, including one pointint to
    `log_file_path` (if set); all these handlers have severity level set to
    `logging.DEBUG`.

    Args:
        logger_name: The identifier of the logger.
        log_file_path: Path to the log file.

    Returns:
        logger: The logger, as configured by the arguments.
    """

    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    current_stream_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    if not current_stream_handlers:
        stream_handler = logging.StreamHandler()
        settings = Inject(get_settings)
        stream_handler.setLevel(settings.FRACTAL_LOGGING_LEVEL)
        stream_handler.setFormatter(LOG_FORMATTER)
        logger.addHandler(stream_handler)

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
            logger.warning(f"Logger {logger_name} has multiple file handlers.")

    return logger


def close_logger(logger: logging.Logger) -> None:
    """
    Close all handlers associated to a `logging.Logger` object

    Arguments:
        logger: The actual logger
    """
    for handle in logger.handlers:
        handle.close()


def reset_logger_handlers(logger: logging.Logger) -> None:
    """
    Close and remove all handlers associated to a `logging.Logger` object

    Arguments:
        logger: The actual logger
    """
    close_logger(logger)
    logger.handlers.clear()


def config_uvicorn_loggers():
    """
    Change the formatter for the uvicorn access/error loggers.

    This is similar to https://stackoverflow.com/a/68864979/19085332. See also
    https://github.com/tiangolo/fastapi/issues/1508.

    This function is meant to work in two scenarios:

    1. The most relevant case is for a `gunicorn` startup command, with
       `--access-logfile` and `--error-logfile` options set.
    2. The case of `fractalctl start` (directly calling `uvicorn`).

    Because of the second use case, we need to check whether uvicorn loggers
    already have a handler. If not, we skip the formatting.
    """

    access_logger = logging.getLogger("uvicorn.access")
    if len(access_logger.handlers) > 0:
        access_logger.handlers[0].setFormatter(LOG_FORMATTER)

    error_logger = logging.getLogger("uvicorn.error")
    if len(error_logger.handlers) > 0:
        error_logger.handlers[0].setFormatter(LOG_FORMATTER)
