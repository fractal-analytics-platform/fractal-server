# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Run simple commands as another user

This module provides a set of tools similar to `subprocess.run`, `glob.glob` or
`os.path.exists`, but extended so that they can be executed on behalf of
another user. Note that this requires appropriate sudo permissions.
"""
import shlex
import subprocess  # nosec
from typing import Optional

from fractal_server.logger import set_logger
from fractal_server.string_tools import validate_cmd

logger = set_logger(__name__)


def _run_command_as_user(
    *,
    cmd: str,
    user: Optional[str] = None,
    encoding: Optional[str] = "utf-8",
    check: bool = False,
) -> subprocess.CompletedProcess:
    """
    Use `sudo -u` to impersonate another user and run a command

    Arguments:
        cmd: Command to be run
        user: User to be impersonated
        encoding: Argument for `subprocess.run`. Note that this must be `None`
                  to have stdout/stderr as bytes.
        check: If `True`, check that `returncode=0` and fail otherwise.

    Raises:
        RuntimeError: if `check=True` and returncode is non-zero.

    Returns:
        res: The return value from `subprocess.run`.
    """
    validate_cmd(cmd)
    logger.debug(f'[_run_command_as_user] {user=}, cmd="{cmd}"')
    if user:
        new_cmd = f"sudo --set-home --non-interactive -u {user} {cmd}"
    else:
        new_cmd = cmd
    res = subprocess.run(  # nosec
        shlex.split(new_cmd),
        capture_output=True,
        encoding=encoding,
    )
    logger.debug(f"[_run_command_as_user] {res.returncode=}")
    logger.debug(f"[_run_command_as_user] {res.stdout=}")
    logger.debug(f"[_run_command_as_user] {res.stderr=}")

    if check and not res.returncode == 0:
        raise RuntimeError(
            f"{cmd=}\n\n{res.returncode=}\n\n{res.stdout=}\n\n{res.stderr=}\n"
        )

    return res


def _mkdir_as_user(*, folder: str, user: str) -> None:
    """
    Create a folder as a different user

    Arguments:
        folder: Absolute path to the folder
        user: User to be impersonated

    Raises:
        RuntimeError: if `user` is not correctly defined, or if subprocess
                      returncode is not 0.
    """
    if not user:
        raise RuntimeError(f"{user=} not allowed in _mkdir_as_user")

    cmd = f"mkdir -p {folder}"
    _run_command_as_user(cmd=cmd, user=user, check=True)


def _path_exists_as_user(*, path: str, user: Optional[str] = None) -> bool:
    """
    Impersonate a user and check if `path` exists via `ls`

    Arguments:
        path: Absolute file/folder path
        user: If not `None`, user to be impersonated
    """
    res = _run_command_as_user(cmd=f"ls {path}", user=user)
    if res.returncode == 0:
        return True
    else:
        return False
