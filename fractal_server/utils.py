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
This module provides general purpose utilities that are not specific to any
subsystem.
"""
import asyncio
from datetime import datetime
from datetime import timezone
from pathlib import Path
from shlex import split as shlex_split
from typing import Optional

from .logger import get_logger


def get_timestamp() -> datetime:
    """
    Get timezone aware timestamp.
    """
    return datetime.now(tz=timezone.utc)


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

    Args:
        cwd:
            The working directory for the command execution.
        command:
            The command to execute.

    Returns:
        stdout:
            The stdout from the command execution.

    Raises:
        RuntimeError: if the process exited with non-zero status. The error
            string is set to the `stderr` of the process.
    """
    command_split = shlex_split(command)
    cmd, *args = command_split

    logger = get_logger(logger_name)
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
