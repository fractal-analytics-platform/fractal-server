"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import asyncio
import logging
import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
from shlex import split as shlex_split
from typing import Optional
from typing import Union
from warnings import warn as _warn

from .config import get_settings
from .syringe import Inject


def file_opener(path: Union[str, Path], flags: int, mode=0o777):
    """
    Custom file opener with umask=0

    Returns a file descriptor with a custom mode set. To achieve this, the
    umask if first set to 0 and then restored.

    Args:
        path:
            The path for which the file descriptor is needed.
        flags:
            The file opening flags to be applied (`rwbt+`).
        mode:
            The mode to apply

    Returns:
        fd:
            File descriptor
    """
    orig_umask = os.umask(0)
    fd = os.open(path, flags, mode=mode)
    os.umask(orig_umask)
    return fd


def close_logger(logger: logging.Logger) -> None:
    """
    Close all FileHandles of `logger`
    """
    for handle in logger.handlers:
        if isinstance(handle, logging.FileHandler):
            handle.close()


def get_timestamp() -> datetime:
    return datetime.now(tz=timezone.utc)


def warn(message):
    """
    Make sure that warnings do not make their way to staing and production
    """
    settings = Inject(get_settings)
    if settings.DEPLOYMENT_TYPE in ["testing", "development"]:
        _warn(message, RuntimeWarning)
    else:
        raise RuntimeError(message)


def set_logger(
    *,
    logger_name: Optional[str] = None,
    log_file_path: Optional[Path] = None,
    level: Optional[int] = None,
    formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    """
    Set up and return a logger
    """
    if not level:
        settings = Inject(get_settings)
        level = settings.FRACTAL_LOGGING_LEVEL
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, mode="a")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


async def execute_command(
    *,
    cwd: Path,
    command: str,
    logger_name: Optional[str] = None,
) -> str:
    """
    Execute arbitrary command

    If the command returns a return code different from zero, a RuntimeError
    containing the stderr is raised.

    Parameters
    ----------
    cwd : Path
        the working directory for the command execution
    command : str
        the command to execute

    Return
    ------
    stdout : str
        the stdout from the command execution
    """
    command_split = shlex_split(command)
    cmd, *args = command_split

    logger = set_logger(logger_name=logger_name)
    proc = await asyncio.create_subprocess_exec(
        cmd,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await proc.communicate()
    logger.debug(f"Subprocess call to: {command}")
    logger.debug(stdout.decode("utf-8"))
    logger.debug(stderr.decode("utf-8"))
    if proc.returncode != 0:
        raise RuntimeError(stderr.decode("utf-8"))
    return stdout.decode("utf-8")
