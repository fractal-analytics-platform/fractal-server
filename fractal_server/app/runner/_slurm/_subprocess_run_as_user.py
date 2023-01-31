import logging
import shlex
import subprocess  # nosec


def _run_command_as_user(*, cmd: str, user: str):
    """
    Use `sudo -u` to impersonate another user and run a command
    """
    # FIXME: turn INFO into DEBUG
    logging.debug(f'[_run_command_as_user] {user=}, cmd="{cmd}"')
    res = subprocess.run(  # nosec
        shlex.split(f"sudo --non-interactive -u {user} {cmd}"),
        capture_output=True,
        encoding="utf-8",
    )
    logging.debug(f"[_run_command_as_user] {res.returncode=}")
    logging.debug(f"[_run_command_as_user] {res.stdout=}")
    logging.debug(f"[_run_command_as_user] {res.stderr=}")
    return res


def _mkdir_as_user(*, folder: str, user: str):
    """
    Create a folder as a different user
    """
    cmd = f"mkdir {folder}"
    res = _run_command_as_user(cmd=cmd, user=user)
    if not res.returncode == 0:
        raise RuntimeError(
            f"{cmd=}\n\n"
            f"{res.returncode=}\n\n"
            f"{res.stdout=}\n\n"
            f"{res.stderr=}\n"
        )
