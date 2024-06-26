import time
from typing import Optional

from fabric import Connection
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError

from ..logger import set_logger
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

logger = set_logger(__name__)

MAX_ATTEMPTS = 5


def get_ssh_connection(
    *,
    host: Optional[str] = None,
    user: Optional[str] = None,
    key_filename: Optional[str] = None,
) -> Connection:
    """
    Create a fabric Connection object based on settings or explicit arguments.
    """
    settings = Inject(get_settings)
    if host is None:
        host = settings.FRACTAL_SLURM_SSH_HOST
    if user is None:
        user = settings.FRACTAL_SLURM_SSH_USER
    if key_filename is None:
        key_filename = settings.FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH

    connection = Connection(
        host=host,
        user=user,
        connect_kwargs={"key_filename": key_filename},
    )
    logger.debug(f"Now created {connection=}.")
    return connection


def run_command_over_ssh(
    *,
    cmd: str,
    connection: Connection,
    max_attempts: int = MAX_ATTEMPTS,
    base_interval: float = 3.0,
) -> str:
    """
    Run a command within an open SSH connection.

    Args:
        cmd: Command to be run
        connection: Fabric connection object

    Returns:
        Standard output of the command, if successful.
    """
    t_0 = time.perf_counter()
    ind_attempt = 0
    while ind_attempt <= max_attempts:
        ind_attempt += 1
        prefix = f"[attempt {ind_attempt}/{max_attempts}]"
        logger.info(f"{prefix} START running '{cmd}' over SSH.")
        try:
            # Case 1: Command runs successfully
            res = connection.run(cmd, hide=True)
            t_1 = time.perf_counter()
            logger.info(
                f"{prefix} END   running '{cmd}' over SSH, "
                f"elapsed {t_1-t_0:.3f}"
            )
            logger.debug(f"STDOUT: {res.stdout}")
            logger.debug(f"STDERR: {res.stderr}")
            return res.stdout
        except NoValidConnectionsError as e:
            # Case 2: Command fails with a connection error
            logger.warning(
                f"{prefix} Running command `{cmd}` over SSH failed.\n"
                f"Original NoValidConnectionError:\n{str(e)}.\n"
                f"{e.errors=}\n"
            )
            if ind_attempt < max_attempts:
                sleeptime = (
                    base_interval**ind_attempt
                )  # FIXME SSH: add jitter?
                logger.warning(
                    f"{prefix} Now sleep {sleeptime:.3f} seconds and continue."
                )
                time.sleep(sleeptime)
                continue
            else:
                logger.error(f"{prefix} Reached last attempt")
                break
        except UnexpectedExit as e:
            # Case 3: Command fails with an actual error
            error_msg = (
                f"{prefix} Running command `{cmd}` over SSH failed.\n"
                f"Original error:\n{str(e)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

    raise ValueError(
        f"Reached last attempt ({max_attempts=}) for running '{cmd}'"
    )


def _mkdir_over_ssh(
    *, folder: str, connection: Connection, parents: bool = True
) -> None:
    """
    Create a folder remotely via SSH.

    Args:
        folder:
        connection:
        parents:
    """

    # FIXME SSH: try using `mkdir` method of `paramiko.SFTPClient`
    if parents:
        cmd = f"mkdir -p {folder}"
    else:
        cmd = f"mkdir {folder}"
    run_command_over_ssh(cmd=cmd, connection=connection)
