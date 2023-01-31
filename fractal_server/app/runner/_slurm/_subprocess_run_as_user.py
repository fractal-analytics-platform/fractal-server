import logging
import shlex
import subprocess  # nosec
from typing import Optional


def _run_command_as_user(
    *, cmd: str, user: str, encoding: Optional[str] = "utf-8"
) -> subprocess.CompletedProcess:
    """
    Use `sudo` to impersonate another user and run a command

    Arguments:
        cmd: Command to be run
        user: User to be impersonated
        encoding: Argument for `subprocess.run`. Note that this must be `None`
                  to have stdout/stderr as bytes.

    Returns:
        res: The return value from `subprocess.run`.
    """
    logging.debug(f'[_run_command_as_user] {user=}, cmd="{cmd}"')
    res = subprocess.run(  # nosec
        shlex.split(f"sudo --non-interactive -u {user} {cmd}"),
        capture_output=True,
        encoding=encoding,
    )
    logging.debug(f"[_run_command_as_user] {res.returncode=}")
    logging.debug(f"[_run_command_as_user] {res.stdout=}")
    logging.debug(f"[_run_command_as_user] {res.stderr=}")
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
        raise RuntimeError("{user=} not allowed in _mkdir_as_user")

    cmd = f"mkdir -p {folder}"
    res = _run_command_as_user(cmd=cmd, user=user)
    if not res.returncode == 0:
        raise RuntimeError(
            f"{cmd=}\n\n"
            f"{res.returncode=}\n\n"
            f"{res.stdout=}\n\n"
            f"{res.stderr=}\n"
        )
