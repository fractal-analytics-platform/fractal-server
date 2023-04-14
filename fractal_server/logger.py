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
This module provides logging utilities.
"""
import logging
from pathlib import Path
from typing import Optional
from typing import Union

from .config import get_settings
from .syringe import Inject


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FORMATTER = logging.Formatter(LOG_FORMAT)


def close_logger(logger: logging.Logger) -> None:
    """
    Close all handlers of a logger
    """
    for handle in logger.handlers:
        handle.close()


def warn(message):
    """
    # FIXME: this is not used

    Custom warning that becomes an error in staging and production deployments

    This works towards assuring that warnings do not make their way to staing
    and production.

    Raises:
        RuntimeError: if the deployment type is not `testing` or `development`.
    """
    settings = Inject(get_settings)
    if settings.DEPLOYMENT_TYPE in ["testing", "development"]:
        logging.warning(message, RuntimeWarning)
    else:
        raise RuntimeError(message)


def set_logger(
    *,
    logger_name: Optional[str] = None,
    log_file_path: Optional[Union[str, Path]] = None,
) -> logging.Logger:
    """
    Set up and return a fractal-server logger

    FIXME docstring

    Args:
        logger_name:
            The identifier of the logger.
        log_file_path:
            Path to the log file.
        formatter:
            Custom formatter.

    Returns:
        logger:
            The logger, as configured by the arguments.
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
        stream_handler.setFormatter(LOG_FORMATTER)
        file_handler.setFormatter(LOG_FORMATTER)
        logger.addHandler(file_handler)
        current_file_handlers = [
            handler
            for handler in logger.handlers
            if isinstance(handler, logging.FileHandler)
        ]
        if len(current_file_handlers) > 1:
            logger.warning("Logger {logger_name} has multiple file handlers.")

    return logger
