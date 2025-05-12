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
import shlex
import subprocess  # nosec
from datetime import datetime
from datetime import timezone
from pathlib import Path

from .logger import get_logger
from .string_tools import validate_cmd


def get_timestamp() -> datetime:
    """
    Get timezone aware timestamp.
    """
    return datetime.now(tz=timezone.utc)


async def execute_command_async(
    *,
    command: str,
    cwd: Path | None = None,
    logger_name: str | None = None,
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
    command_split = shlex.split(command)
    cmd, *args = command_split

    logger = get_logger(logger_name)
    cwd_kwarg = dict() if cwd is None else dict(cwd=cwd)
    proc = await asyncio.create_subprocess_exec(
        cmd,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **cwd_kwarg,
    )
    stdout, stderr = await proc.communicate()
    logger.debug(f"Subprocess call to: {command}")
    logger.debug(stdout.decode("utf-8"))
    logger.debug(stderr.decode("utf-8"))
    if proc.returncode != 0:
        raise RuntimeError(stderr.decode("utf-8"))
    return stdout.decode("utf-8")


def execute_command_sync(
    *,
    command: str,
    logger_name: str | None = None,
    allow_char: str | None = None,
) -> str:
    """
    Execute arbitrary command

    If the command returns a return code different from zero, a `RuntimeError`
    is raised.

    Arguments:
        command: Command to be executed.
        logger_name: Name of the logger.
        allow_char: Argument propagated to `validate_cmd`.
    """
    logger = get_logger(logger_name)
    logger.debug(f"START subprocess call to '{command}'")
    validate_cmd(command=command, allow_char=allow_char)
    res = subprocess.run(  # nosec
        shlex.split(command),
        capture_output=True,
        encoding="utf-8",
    )
    returncode = res.returncode
    stdout = res.stdout
    stderr = res.stderr
    if res.returncode != 0:
        logger.debug(f"ERROR in subprocess call to '{command}'")
        raise RuntimeError(
            f"Command {command} failed.\n"
            f"returncode={res.returncode}\n"
            "STDOUT:\n"
            f"{stdout}\n"
            "STDERR:\n"
            f"{stderr}\n"
        )
    logger.debug(f"{returncode=}")
    logger.debug("STDOUT:")
    logger.debug(stdout)
    logger.debug("STDERR:")
    logger.debug(stderr)
    logger.debug(f"END   subprocess call to '{command}'")
    return stdout
