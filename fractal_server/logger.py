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

from .config import get_settings
from .syringe import Inject


def close_logger(logger: logging.Logger) -> None:
    """
    Close all FileHandles of a logger, if any.
    """
    for handle in logger.handlers:
        if isinstance(handle, logging.FileHandler):
            handle.close()


def warn(message):
    """
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
    log_file_path: Optional[Path] = None,
    formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    """
    Set up and return a logger

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

    ch = logging.StreamHandler()
    ch.setLevel(Inject(get_settings).FRACTAL_LOGGING_LEVEL)
    formatter = logging.Formatter(
        "STREAMHANDLER %(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )  # noqa
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if log_file_path:
        fh = logging.FileHandler(log_file_path, mode="a")
        formatter = logging.Formatter(
            "FILEHANDLER %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )  # noqa
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    from devtools import debug

    debug(logger)
    debug(vars(logger))

    return logger
