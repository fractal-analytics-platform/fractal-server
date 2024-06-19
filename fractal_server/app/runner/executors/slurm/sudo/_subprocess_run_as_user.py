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

from ......logger import set_logger

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
    logger.debug(f'[_run_command_as_user] {user=}, cmd="{cmd}"')
    if user:
        new_cmd = f"sudo --non-interactive -u {user} {cmd}"
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
            f"{cmd=}\n\n"
            f"{res.returncode=}\n\n"
            f"{res.stdout=}\n\n"
            f"{res.stderr=}\n"
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


def _glob_as_user(
    *, folder: str, user: str, startswith: Optional[str] = None
) -> list[str]:
    """
    Run `ls` in a folder (as a user) and filter results

    Execute `ls` on a folder (impersonating a user, if `user` is not `None`)
    and select results that start with `startswith` (if not `None`).

    Arguments:
        folder: Absolute path to the folder
        user: If not `None`, the user to be impersonated via `sudo -u`
        startswith: If not `None`, this is used to filter output of `ls`.
    """

    res = _run_command_as_user(cmd=f"ls {folder}", user=user, check=True)
    output = res.stdout.split()
    if startswith:
        output = [f for f in output if f.startswith(startswith)]
    return output


def _glob_as_user_strict(
    *,
    folder: str,
    user: str,
    startswith: str,
) -> list[str]:
    """
    Run `ls` in a folder (as a user) and filter results

    Execute `ls` on a folder (impersonating a user, if `user` is not `None`)
    and select results that comply with a set of rules. They all start with
    `startswith` (if not `None`), and they match one of the known filename
    patterns. See details in
    https://github.com/fractal-analytics-platform/fractal-server/issues/1240


    Arguments:
        folder: Absolute path to the folder
        user: If not `None`, the user to be impersonated via `sudo -u`
        startswith: If not `None`, this is used to filter output of `ls`.
    """

    res = _run_command_as_user(cmd=f"ls {folder}", user=user, check=True)
    output = res.stdout.split()

    new_output = []
    known_filenames = [
        f"{startswith}{suffix}"
        for suffix in [".args.json", ".metadiff.json", ".err", ".out", ".log"]
    ]
    for filename in output:
        if filename in known_filenames:
            new_output.append(filename)
        elif filename.startswith(f"{startswith}_out_") and filename.endswith(
            ".pickle"
        ):
            new_output.append(filename)

    return new_output


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
