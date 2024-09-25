import logging
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any
from typing import Generator
from typing import Literal
from typing import Optional

import paramiko.sftp_client
from fabric import Connection
from fabric import Result
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError

from ..logger import get_logger
from ..logger import set_logger
from fractal_server.string_tools import validate_cmd


class FractalSSHTimeoutError(RuntimeError):
    pass


class FractalSSHListTimeoutError(RuntimeError):
    pass


logger = set_logger(__name__)


class FractalSSH(object):
    """
    FIXME SSH: Fix docstring

    Attributes:
        _lock:
        connection:
        default_lock_timeout:
        logger_name:
    """

    _lock: Lock
    _connection: Connection
    default_lock_timeout: float
    default_max_attempts: int
    default_base_interval: float
    logger_name: str

    def __init__(
        self,
        connection: Connection,
        default_timeout: float = 250,
        default_max_attempts: int = 5,
        default_base_interval: float = 3.0,
        logger_name: str = __name__,
    ):
        self._lock = Lock()
        self._connection = connection
        self.default_lock_timeout = default_timeout
        self.default_base_interval = default_base_interval
        self.default_max_attempts = default_max_attempts
        self.logger_name = logger_name
        set_logger(self.logger_name)

    @contextmanager
    def acquire_timeout(
        self, timeout: float
    ) -> Generator[Literal[True], Any, None]:
        self.logger.debug(f"Trying to acquire lock, with {timeout=}")
        result = self._lock.acquire(timeout=timeout)
        try:
            if not result:
                self.logger.error("Lock was *NOT* acquired.")
                raise FractalSSHTimeoutError(
                    f"Failed to acquire lock within {timeout} seconds"
                )
            self.logger.debug("Lock was acquired.")
            yield result
        finally:
            if result:
                self._lock.release()
                self.logger.debug("Lock was released")

    @property
    def is_connected(self) -> bool:
        return self._connection.is_connected

    @property
    def logger(self) -> logging.Logger:
        return get_logger(self.logger_name)

    def put(
        self, *args, lock_timeout: Optional[float] = None, **kwargs
    ) -> Result:
        actual_lock_timeout = self.default_lock_timeout
        if lock_timeout is not None:
            actual_lock_timeout = lock_timeout
        with self.acquire_timeout(timeout=actual_lock_timeout):
            return self._connection.put(*args, **kwargs)

    def get(
        self, *args, lock_timeout: Optional[float] = None, **kwargs
    ) -> Result:
        actual_lock_timeout = self.default_lock_timeout
        if lock_timeout is not None:
            actual_lock_timeout = lock_timeout
        with self.acquire_timeout(timeout=actual_lock_timeout):
            return self._connection.get(*args, **kwargs)

    def run(
        self, *args, lock_timeout: Optional[float] = None, **kwargs
    ) -> Any:
        actual_lock_timeout = self.default_lock_timeout
        if lock_timeout is not None:
            actual_lock_timeout = lock_timeout
        with self.acquire_timeout(timeout=actual_lock_timeout):
            return self._connection.run(*args, **kwargs)

    def sftp(self) -> paramiko.sftp_client.SFTPClient:
        return self._connection.sftp()

    def check_connection(self) -> None:
        """
        Open the SSH connection and handle exceptions.

        This function can be called from within other functions that use
        `connection`, so that we can provide a meaningful error in case the
        SSH connection cannot be opened.
        """
        if not self._connection.is_connected:
            try:
                self._connection.open()
            except Exception as e:
                raise RuntimeError(
                    f"Cannot open SSH connection. Original error:\n{str(e)}"
                )

    def close(self) -> None:
        """
        Aggressively close `self._connection`.

        When `Connection.is_connected` is `False`, `Connection.close()` does
        not call `Connection.client.close()`. Thus we do this explicitly here,
        because we observed cases where `is_connected=False` but the underlying
        `Transport` object was not closed.
        """

        self._connection.close()

        if self._connection.client is not None:
            self._connection.client.close()

    def run_command(
        self,
        *,
        cmd: str,
        allow_char: Optional[str] = None,
        max_attempts: Optional[int] = None,
        base_interval: Optional[int] = None,
        lock_timeout: Optional[int] = None,
    ) -> str:
        """
        Run a command within an open SSH connection.

        Args:
            cmd: Command to be run
            allow_char: Forbidden chars to allow for this command
            max_attempts:
            base_interval:
            lock_timeout:

        Returns:
            Standard output of the command, if successful.
        """

        validate_cmd(cmd, allow_char=allow_char)

        actual_max_attempts = self.default_max_attempts
        if max_attempts is not None:
            actual_max_attempts = max_attempts

        actual_base_interval = self.default_base_interval
        if base_interval is not None:
            actual_base_interval = base_interval

        actual_lock_timeout = self.default_lock_timeout
        if lock_timeout is not None:
            actual_lock_timeout = lock_timeout

        t_0 = time.perf_counter()
        ind_attempt = 0
        while ind_attempt <= actual_max_attempts:
            ind_attempt += 1
            prefix = f"[attempt {ind_attempt}/{actual_max_attempts}]"
            self.logger.info(f"{prefix} START running '{cmd}' over SSH.")
            try:
                # Case 1: Command runs successfully
                res = self.run(
                    cmd, lock_timeout=actual_lock_timeout, hide=True
                )
                t_1 = time.perf_counter()
                self.logger.info(
                    f"{prefix} END   running '{cmd}' over SSH, "
                    f"elapsed {t_1-t_0:.3f}"
                )
                self.logger.debug(f"STDOUT: {res.stdout}")
                self.logger.debug(f"STDERR: {res.stderr}")
                return res.stdout
            except NoValidConnectionsError as e:
                # Case 2: Command fails with a connection error
                self.logger.warning(
                    f"{prefix} Running command `{cmd}` over SSH failed.\n"
                    f"Original NoValidConnectionError:\n{str(e)}.\n"
                    f"{e.errors=}\n"
                )
                if ind_attempt < actual_max_attempts:
                    sleeptime = actual_base_interval**ind_attempt
                    self.logger.warning(
                        f"{prefix} Now sleep {sleeptime:.3f} "
                        "seconds and continue."
                    )
                    time.sleep(sleeptime)
                else:
                    self.logger.error(f"{prefix} Reached last attempt")
                    break
            except UnexpectedExit as e:
                # Case 3: Command fails with an actual error
                error_msg = (
                    f"{prefix} Running command `{cmd}` over SSH failed.\n"
                    f"Original error:\n{str(e)}."
                )
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
            except Exception as e:
                self.logger.error(
                    f"Running command `{cmd}` over SSH failed.\n"
                    f"Original Error:\n{str(e)}."
                )
                raise e

        raise RuntimeError(
            f"Reached last attempt ({max_attempts=}) for running "
            f"'{cmd}' over SSH"
        )

    def send_file(
        self,
        *,
        local: str,
        remote: str,
        logger_name: Optional[str] = None,
        lock_timeout: Optional[float] = None,
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
            self.put(local=local, remote=remote, lock_timeout=lock_timeout)
        except Exception as e:
            logger = get_logger(logger_name=logger_name)
            logger.error(
                f"Transferring {local=} to {remote=} over SSH failed.\n"
                f"Original Error:\n{str(e)}."
            )
            raise e

    def mkdir(self, *, folder: str, parents: bool = True) -> None:
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
        self.run_command(cmd=cmd)

    def remove_folder(
        self,
        *,
        folder: str,
        safe_root: str,
    ) -> None:
        """
        Removes a folder remotely via SSH.

        This functions calls `rm -r`, after a few checks on `folder`.

        Args:
            folder: Absolute path to a folder that should be removed.
            safe_root: If `folder` is not a subfolder of the absolute
                `safe_root` path, raise an error.
        """
        validate_cmd(folder)
        validate_cmd(safe_root)

        if " " in folder:
            raise ValueError(f"folder='{folder}' includes whitespace.")
        elif " " in safe_root:
            raise ValueError(f"safe_root='{safe_root}' includes whitespace.")
        elif not Path(folder).is_absolute():
            raise ValueError(f"{folder=} is not an absolute path.")
        elif not Path(safe_root).is_absolute():
            raise ValueError(f"{safe_root=} is not an absolute path.")
        elif not (
            Path(folder).resolve().is_relative_to(Path(safe_root).resolve())
        ):
            raise ValueError(f"{folder=} is not a subfolder of {safe_root=}.")
        else:
            cmd = f"rm -r {folder}"
            self.run_command(cmd=cmd)

    def write_remote_file(
        self,
        *,
        path: str,
        content: str,
        lock_timeout: Optional[float] = None,
    ) -> None:
        """
        Open a remote file via SFTP and write it.

        Args:
            path: Absolute path
            contents: File contents
            lock_timeout:
        """
        actual_lock_timeout = self.default_lock_timeout
        if lock_timeout is not None:
            actual_lock_timeout = lock_timeout
        with self.acquire_timeout(timeout=actual_lock_timeout):
            with self.sftp().open(filename=path, mode="w") as f:
                f.write(content)


class FractalSSHList(object):
    """
    Collection of `FractalSSH` objects

    Attributes are all private, and access to this collection must be
    through methods (mostly the `get` one).

    Attributes:
        _data:
            Mapping of unique keys (the SSH-credentials tuples) to
            `FractalSSH` objects.
        _lock:
            A `threading.Lock object`, to be acquired when changing `_data`.
        _timeout: Timeout for `_lock` acquisition.
        _logger_name: Logger name.
    """

    _data: dict[tuple[str, str, str], FractalSSH]
    _lock: Lock
    _timeout: float
    _logger_name: str

    def __init__(
        self,
        *,
        timeout: float = 5.0,
        logger_name: str = "fractal_server.FractalSSHList",
    ):
        self._lock = Lock()
        self._data = {}
        self._timeout = timeout
        self._logger_name = logger_name
        set_logger(self._logger_name)

    @property
    def logger(self) -> logging.Logger:
        """
        This property exists so that we never have to propagate the
        `Logger` object.
        """
        return get_logger(self._logger_name)

    @property
    def size(self) -> int:
        """
        Number of current key-value pairs in `self._data`.
        """
        return len(self._data.values())

    def get(self, *, host: str, user: str, key_path: str) -> FractalSSH:
        """
        Get the `FractalSSH` for the current credentials, or create one.

        Note: Changing `_data` requires acquiring `_lock`.

        Arguments:
            host:
            user:
            key_path:
        """
        key = (host, user, key_path)
        fractal_ssh = self._data.get(key, None)
        if fractal_ssh is not None:
            self.logger.info(
                f"Return existing FractalSSH object for {user}@{host}"
            )
            return fractal_ssh
        else:
            self.logger.info(f"Add new FractalSSH object for {user}@{host}")
            connection = Connection(
                host=host,
                user=user,
                forward_agent=False,
                connect_kwargs={
                    "key_filename": key_path,
                    "look_for_keys": False,
                },
            )
            with self.acquire_lock_with_timeout():
                self._data[key] = FractalSSH(connection=connection)
                return self._data[key]

    def contains(
        self,
        *,
        host: str,
        user: str,
        key_path: str,
    ) -> bool:
        """
        Return whether a given key is present in the collection.

        Arguments:
            host:
            user:
            key_path:
        """
        key = (host, user, key_path)
        return key in self._data.keys()

    def remove(
        self,
        *,
        host: str,
        user: str,
        key_path: str,
    ) -> None:
        """
        Remove a key from `_data` and close the corresponding connection.

        Note: Changing `_data` requires acquiring `_lock`.

        Arguments:
            host:
            user:
            key_path:
        """
        key = (host, user, key_path)
        with self.acquire_lock_with_timeout():
            self.logger.info(
                f"Removing FractalSSH object for {user}@{host} "
                "from collection."
            )
            fractal_ssh_obj = self._data.pop(key)
            self.logger.info(
                f"Closing FractalSSH object for {user}@{host} "
                f"({fractal_ssh_obj.is_connected=})."
            )
            fractal_ssh_obj.close()

    def close_all(self, *, timeout: float = 5.0):
        """
        Close all `FractalSSH` objects in the collection.

        Arguments:
            timeout:
                Timeout for `FractalSSH._lock` acquisition, to be obtained
                before closing.
        """
        for key, fractal_ssh_obj in self._data.items():
            host, user, _ = key[:]
            self.logger.info(
                f"Closing FractalSSH object for {user}@{host} "
                f"({fractal_ssh_obj.is_connected=})."
            )
            with fractal_ssh_obj.acquire_timeout(timeout=timeout):
                fractal_ssh_obj.close()

    @contextmanager
    def acquire_lock_with_timeout(self) -> Generator[Literal[True], Any, None]:
        self.logger.debug(
            f"Trying to acquire lock, with timeout {self._timeout} s"
        )
        result = self._lock.acquire(timeout=self._timeout)
        try:
            if not result:
                self.logger.error("Lock was *NOT* acquired.")
                raise FractalSSHListTimeoutError(
                    f"Failed to acquire lock within {self._timeout} ss"
                )
            self.logger.debug("Lock was acquired.")
            yield result
        finally:
            if result:
                self._lock.release()
                self.logger.debug("Lock was released")
