import logging
import shlex
import subprocess  # nosec
from typing import Optional


def _run_command_as_user(
    *, cmd: str, user: str, encoding: Optional[str] = "utf-8"
):
    """
    Use `sudo -u` to impersonate another user and run a command

    FIXME docstring
    """
    # FIXME: turn INFO into DEBUG
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


def _mkdir_as_user(*, folder: str, user: str):
    """
    Create a folder as a different user
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
