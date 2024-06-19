import time

from fabric.connection import Connection
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError

from ......logger import set_logger
from ......syringe import Inject
from fractal_server.config import get_settings

# from ....exceptions import JobExecutionError


logger = set_logger(__name__)

MAX_ATTEMPTS = 4


def _run_command_over_ssh(
    *,
    cmd: str,
    connection: Connection,
    max_attempts: int = 1,
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
                sleeptime = base_interval**ind_attempt  # FIXME add jitter
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
            raise ValueError(error_msg)  # FIXME switch to JobExecutionError
            # raise JobExecutionError(info=error_msg)

    raise ValueError(f"Reached last attempt for running '{cmd}'")  # FIXME
    # raise JobExecutionError(info=error_msg)


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
