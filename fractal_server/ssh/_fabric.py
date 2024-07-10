import time
from contextlib import contextmanager
from threading import Lock
from typing import Any
from typing import Optional

from fabric import Connection
from fabric import Result
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError

from ..logger import get_logger
from ..logger import set_logger
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

logger = set_logger(__name__)

MAX_ATTEMPTS = 5


class TimeoutException(Exception):
    pass


@contextmanager
def acquire_timeout(lock: Lock, timeout: int) -> Any:
    logger.debug(f"Trying to acquire lock, with {timeout=}")
    result = lock.acquire(timeout=timeout)
    try:
        if not result:
            raise TimeoutException(
                f"Failed to acquire lock within {timeout} seconds"
            )
        logger.debug("Lock was acquired.")
        yield result
    finally:
        if result:
            lock.release()
            logger.debug("Lock was released")


class FractalSSH(object):
    lock: Lock
    connection: Connection
    default_timeout: int

    # FIXME SSH: maybe extend the actual_timeout logic to other methods

    def __init__(self, connection: Connection, default_timeout: int = 250):
        self.lock = Lock()
        self.conn = connection
        self.default_timeout = default_timeout

    @property
    def is_connected(self) -> bool:
        return self.conn.is_connected

    def put(self, *args, timeout: Optional[int] = None, **kwargs) -> Result:
        actual_timeout = timeout or self.default_timeout
        with acquire_timeout(self.lock, timeout=actual_timeout):
            return self.conn.put(*args, **kwargs)

    def get(self, *args, **kwargs) -> Result:
        with acquire_timeout(self.lock, timeout=self.default_timeout):
            return self.conn.get(*args, **kwargs)

    def run(self, *args, **kwargs) -> Any:
        with acquire_timeout(self.lock, timeout=self.default_timeout):
            return self.conn.run(*args, **kwargs)

    def close(self):
        return self.conn.close()

    def sftp(self):
        return self.conn.sftp()

    def check_connection(self) -> None:
        """
        Open the SSH connection and handle exceptions.

        This function can be called from within other functions that use
        `connection`, so that we can provide a meaningful error in case the
        SSH connection cannot be opened.
        """
        if not self.conn.is_connected:
            try:
                self.conn.open()
            except Exception as e:
                raise RuntimeError(
                    f"Cannot open SSH connection (original error: '{str(e)}')."
                )


def get_ssh_connection(
    *,
    host: Optional[str] = None,
    user: Optional[str] = None,
    key_filename: Optional[str] = None,
) -> Connection:
    """
    Create a `fabric.Connection` object based on fractal-server settings
    or explicit arguments.

    Args:
        host:
        user:
        key_filename:

    Returns:
        Fabric connection object
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
    fractal_ssh: FractalSSH,
    max_attempts: int = MAX_ATTEMPTS,
    base_interval: float = 3.0,
) -> str:
    """
    Run a command within an open SSH connection.

    Args:
        cmd: Command to be run
        fractal_ssh: FractalSSH connection object with custom lock

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
            res = fractal_ssh.run(cmd, hide=True)
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
                sleeptime = base_interval**ind_attempt
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
        except Exception as e:
            logger.error(
                f"Running command `{cmd}` over SSH failed.\n"
                f"Original Error:\n{str(e)}."
            )
            raise e

    raise ValueError(
        f"Reached last attempt ({max_attempts=}) for running '{cmd}' over SSH"
    )


def put_over_ssh(
    *,
    local: str,
    remote: str,
    fractal_ssh: FractalSSH,
    logger_name: Optional[str] = None,
) -> None:
    """
    Transfer a file via SSH

    Args:
        local: Local path to file
        remote: Target path on remote host
        fractal_ssh: FractalSSH connection object with custom lock
        logger_name: Name of the logger

    """
    try:
        fractal_ssh.put(local=local, remote=remote)
    except Exception as e:
        logger = get_logger(logger_name=logger_name)
        logger.error(
            f"Transferring {local=} to {remote=} over SSH failed.\n"
            f"Original Error:\n{str(e)}."
        )
        raise e


def _mkdir_over_ssh(
    *, folder: str, fractal_ssh: FractalSSH, parents: bool = True
) -> None:
    """
    Create a folder remotely via SSH.

    Args:
        folder:
        fractal_ssh:
        parents:
    """
    # FIXME SSH: try using `mkdir` method of `paramiko.SFTPClient`
    if parents:
        cmd = f"mkdir -p {folder}"
    else:
        cmd = f"mkdir {folder}"
    run_command_over_ssh(cmd=cmd, fractal_ssh=fractal_ssh)
