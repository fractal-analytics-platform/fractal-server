import time

from fabric.connection import Connection
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError

from .....logger import set_logger
from .....syringe import Inject
from ...exceptions import JobExecutionError
from fractal_server.config import get_settings


logger = set_logger(__name__)


def _run_command_over_ssh(
    *,
    cmd: str,
    connection: Connection,
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
    logger.info(f"START running '{cmd}' over SSH.")
    try:
        res = connection.run(cmd, hide=True)
    except UnexpectedExit as e:
        error_msg = (
            f"Running command `{cmd}` over SSH failed.\n"
            f"Original error:\n{str(e)}."
        )
        logger.error(error_msg)
        # FIXME switch to JobExecutionError
        raise ValueError(error_msg)
        raise JobExecutionError(info=error_msg)

    except NoValidConnectionsError as e:
        error_msg = (
            f"Running command `{cmd}` over SSH failed.\n"
            f"Original NoValidConnectionError:\n{str(e)}.\n"
            f"{e.errors=}\n"
        )
        logger.error(error_msg)
        # FIXME switch to JobExecutionError
        raise ValueError(error_msg)
        raise JobExecutionError(info=error_msg)

    t_1 = time.perf_counter()
    logger.info(f"END   running '{cmd}' over SSH, elapsed {t_1-t_0:.3f}")
    logger.debug(f"STDOUT: {res.stdout}")
    logger.debug(f"STDERR: {res.stderr}")
    return res.stdout


def _mkdir_over_ssh(*, folder: str, parents: bool = True) -> None:
    """
    Create a folder remotely via SSH.

    Args:
        folder:
    """

    # FIXME: paramiko SFTPClient has a mkdir method

    if parents:
        cmd = f"mkdir -p {folder}"
    else:
        cmd = f"mkdir {folder}"

    settings = Inject(get_settings)
    timeout = 3
    with Connection(
        host=settings.FRACTAL_SLURM_SSH_HOST,
        user=settings.FRACTAL_SLURM_SSH_USER,
        connect_kwargs={
            "key_filename": settings.FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH
        },
        connect_timeout=timeout,
    ) as connection:
        _run_command_over_ssh(cmd=cmd, connection=connection)
