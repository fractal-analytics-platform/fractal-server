import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from devtools import debug
from fabric.connection import Connection
from paramiko.ssh_exception import NoValidConnectionsError

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.ssh._fabric import FractalSSHTimeoutError
from fractal_server.ssh._fabric import _acquire_lock_with_timeout

logger = set_logger(__file__)


def test_acquire_lock():
    """
    Test that the lock cannot be acquired twice.
    """
    with Connection("localhost") as connection:
        fake_fractal_ssh = FractalSSH(connection=connection)
        fake_fractal_ssh._lock.acquire(timeout=0)
        with pytest.raises(FractalSSHTimeoutError) as e:
            with _acquire_lock_with_timeout(
                lock=fake_fractal_ssh._lock,
                timeout=0.1,
                label="fail",
                pid=12345,
                logger_name="logger",
            ):
                pass
        print(e)


def test_fail_and_raise(tmp_path: Path, caplog):
    """
    Test Exception when `e.errors` is not an iterable.
    """

    class MyError(Exception):
        errors = 0

    class MockFractalSSH(FractalSSH):
        @property
        def _sftp_unsafe(self):
            raise MyError()

    LOGGER_NAME = "invalid_ssh"
    with Connection(
        host="localhost",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        mocked_fractal_ssh = MockFractalSSH(
            connection=connection, logger_name=LOGGER_NAME
        )

        logger = logging.getLogger(LOGGER_NAME)
        logger.propagate = True

        with pytest.raises(MyError):
            mocked_fractal_ssh.send_file(
                local="/invalid/local",
                remote="/invalid/remote",
            )
        log_text = caplog.text
        assert "Unexpected" in log_text
        assert "'int' object is not iterable" in log_text


@pytest.mark.container
@pytest.mark.ssh
def test_run_command(fractal_ssh: FractalSSH, ssh_username):
    """
    Basic working of `run_command` method.
    """

    # Successful remote execution
    stdout = fractal_ssh.run_command(
        cmd="whoami",
        lock_timeout=1.0,
    )
    assert stdout.strip("\n") == ssh_username

    # When the remotely-executed command fails, a RuntimeError is raised.
    with pytest.raises(
        RuntimeError, match="Encountered a bad command exit code"
    ):
        fractal_ssh.run_command(
            cmd="ls --invalid-option",
            lock_timeout=1.0,
        )


@pytest.mark.container
@pytest.mark.ssh
def test_run_command_concurrency(fractal_ssh: FractalSSH):
    """
    Test locking feature for `run_command` method.
    """

    # Useful auxiliary function
    def _run_sleep(label: str, lock_timeout: float):
        logger.info(f"Start running with {label=} and {lock_timeout=}")
        fractal_ssh.run_command(cmd="sleep 1", lock_timeout=lock_timeout)

    # Submit two commands to be run, with a large timeout for lock acquisition
    with ThreadPoolExecutor(max_workers=2) as executor:
        results_iterator = executor.map(_run_sleep, ["A", "B"], [2.0, 2.0])
        list(results_iterator)

    # Submit two commands to be run, with a small timeout for lock acquisition
    with ThreadPoolExecutor(max_workers=2) as executor:
        results_iterator = executor.map(_run_sleep, ["C", "D"], [0.1, 0.1])
        with pytest.raises(
            FractalSSHTimeoutError, match="Failed to acquire lock"
        ):
            list(results_iterator)


@pytest.mark.container
@pytest.mark.ssh
def test_run_command_retries(fractal_ssh: FractalSSH, ssh_username):
    """
    Test the multiple-attempts logic of `run_command`.
    """

    class MockFractalSSH(FractalSSH):
        """
        Mock FractalSSH object, such that the first call to `run` always fails.
        """

        please_raise: bool

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.please_raise = True

        # This mock modifies the _run behaviour
        def _run(self, *args, **kwargs):
            if self.please_raise:
                # Set `please_raise=False`, so that next call will go through
                self.please_raise = False
                # Construct a NoValidConnectionsError. Note that we prepare an
                # `errors` attribute with the appropriate type, but with no
                # meaningful content
                errors = {("str", 1): ("str", 1, 1, 1)}
                raise NoValidConnectionsError(errors=errors)
            return super()._run(*args, **kwargs)

    mocked_fractal_ssh = MockFractalSSH(connection=fractal_ssh._connection)

    # Call with max_attempts=2 goes through (note that we have to reset
    # `please_raise`)
    mocked_fractal_ssh.please_raise = True
    stdout = mocked_fractal_ssh.run_command(cmd="whoami")
    assert stdout.strip() == ssh_username


@pytest.mark.container
@pytest.mark.ssh
def test_file_transfer(fractal_ssh: FractalSSH, tmp_path: Path):
    """
    Test basic working of `send_file` and `fetch_file` methods.
    """
    local_file_old = (tmp_path / "local_old").as_posix()
    local_file_new = (tmp_path / "local_new").as_posix()
    with open(local_file_old, "w") as f:
        f.write("hi there\n")

    # Send file
    fractal_ssh.send_file(local=local_file_old, remote="remote_file")

    # Get back file (note: we include the `lock_timeout` argument only
    # for coverage of the corresponding conditional branch)
    fractal_ssh.fetch_file(
        remote="remote_file", local=local_file_new, lock_timeout=1.0
    )
    assert Path(local_file_new).is_file()

    # Fail in fetching file
    with pytest.raises(FileNotFoundError):
        fractal_ssh.fetch_file(
            remote="missing_remote_file",
            local=(tmp_path / "local_version").as_posix(),
            lock_timeout=1.0,
        )


@pytest.mark.container
@pytest.mark.ssh
def test_send_file_concurrency(fractal_ssh: FractalSSH, tmp_path: Path):
    local_file = (tmp_path / "local").as_posix()
    with open(local_file, "w") as f:
        f.write("x" * 10_000)

    def _send_file(remote: str, lock_timeout: float):
        logger.info(f"Send file to {remote=}.")
        fractal_ssh.send_file(
            local=local_file,
            remote=remote,
            lock_timeout=lock_timeout,
        )

    # Try running two concurrent runs, with long lock timeout
    with ThreadPoolExecutor(max_workers=2) as executor:
        results_iterator = executor.map(
            _send_file, ["remote1", "remote2"], [1.0, 1.0]
        )
        list(results_iterator)

    # Try running two concurrent runs and fail, due to short lock timeout
    with ThreadPoolExecutor(max_workers=2) as executor:
        results_iterator = executor.map(
            _send_file, ["remote3", "remote4"], [0.0, 0.0]
        )
        with pytest.raises(FractalSSHTimeoutError) as e:
            list(results_iterator)
        assert "Failed to acquire lock" in str(e.value)


@pytest.mark.container
@pytest.mark.ssh
def test_folder_utils(tmp777_path, fractal_ssh: FractalSSH):
    """
    Test basic working of `mkdir` and `remove_folder` methods.
    """

    # Define folder
    folder = (tmp777_path / "nested/folder").as_posix()

    # Check that folder does not exist
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.run_command(cmd=f"ls {folder}")
    print(e.value)

    # Try to create folder, without parents options
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.mkdir(folder=folder, parents=False)
    print(e.value)

    # Create folder
    fractal_ssh.mkdir(folder=folder, parents=True)

    # Check that folder exists
    stdout = fractal_ssh.run_command(cmd=f"ls {folder}")
    print(stdout)
    print()

    # Remove folder
    fractal_ssh.remove_folder(
        folder=folder, safe_root=tmp777_path.parent.as_posix()
    )

    # Check that folder does not exist
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.run_command(cmd=f"ls {folder}")
    print(e.value)

    # Check that removing a missing folder fails
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.remove_folder(
            folder="/invalid/something",
            safe_root="/invalid",
        )
    print(e.value)


def test_remove_folder_input_validation():
    """
    Test input validation of `remove_folder` method.
    """
    with Connection(host="localhost") as connection:
        fake_fractal_ssh = FractalSSH(connection=connection)

        # Folders which are just invalid
        invalid_folders = [
            None,
            "   /somewhere",
            "/ somewhere",
            "somewhere",
            "$(pwd)",
            "`pwd`",
        ]
        for folder in invalid_folders:
            with pytest.raises(ValueError) as e:
                fake_fractal_ssh.remove_folder(folder=folder, safe_root="/")
            print(e.value)

        # Folders which are just invalid
        invalid_folders = [
            None,
            "   /somewhere",
            "/ somewhere",
            "somewhere",
            "$(pwd)",
            "`pwd`",
        ]
        for safe_root in invalid_folders:
            with pytest.raises(ValueError) as e:
                fake_fractal_ssh.remove_folder(
                    folder="/tmp/something",
                    safe_root=safe_root,
                )
            print(e.value)

        # Folders which are not relative to the accepted root
        with pytest.raises(ValueError) as e:
            fake_fractal_ssh.remove_folder(folder="/", safe_root="/tmp")
        print(e.value)

        with pytest.raises(ValueError) as e:
            fake_fractal_ssh.remove_folder(
                folder="/actual_root/../something",
                safe_root="/actual_root",
            )
        print(e.value)


@pytest.mark.container
@pytest.mark.ssh
def test_write_remote_file(fractal_ssh: FractalSSH, tmp777_path: Path):
    path = tmp777_path / "file"
    content = "this is what goes into the file"
    fractal_ssh.write_remote_file(
        path=path.as_posix(), content=content, lock_timeout=100
    )
    assert path.exists()
    with path.open("r") as f:
        assert f.read() == content


def test_novalidconnectionserror_in_sftp_methods(caplog):
    """
    Test `NoValidConnectionError`s in SFTP-based methods.
    """

    with Connection(
        host="localhost",
        port="8022",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        LOGGER_NAME = "invalid_ssh"
        fractal_ssh = FractalSSH(
            connection=connection,
            logger_name=LOGGER_NAME,
        )
        logger = logging.getLogger(LOGGER_NAME)
        logger.propagate = True

        # Fail in several methods, because connection is closed

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.write_remote_file(path="/invalid", content="..")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.send_file(local="/invalid", remote="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.fetch_file(local="/invalid", remote="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.remote_exists(path="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.read_remote_json_file(
                filepath="/invalid",
            )
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(RuntimeError):
            fractal_ssh.read_remote_text_file(
                filepath="/invalid",
            )
        assert "NoValidConnectionsError" in caplog.text

        fractal_ssh.close()


@pytest.mark.container
@pytest.mark.ssh
def test_remote_file_exists(fractal_ssh: FractalSSH, tmp777_path: Path):
    remote_folder = (tmp777_path / "folder").as_posix()
    remote_file = (tmp777_path / "folder/file").as_posix()

    assert not fractal_ssh.remote_exists(path=remote_folder)
    assert not fractal_ssh.remote_exists(path=remote_file)

    Path(remote_folder).mkdir()
    assert fractal_ssh.remote_exists(path=remote_folder)
    assert not fractal_ssh.remote_exists(path=remote_file)

    with open(remote_file, "w") as f:
        f.write("hello\n")
    assert fractal_ssh.remote_exists(path=remote_folder)
    assert fractal_ssh.remote_exists(path=remote_file)


@pytest.mark.container
@pytest.mark.ssh
def test_closed_socket(
    slurmlogin_ip,
    ssh_keys,
    ssh_alive,
    tmp777_path: Path,
):
    """
    This test reproduces the situation where sockets for the paramiko/fabric
    connections are closed (e.g. due to a restart of the SSH service), but
    the corresponding Python objects remain active.

    The `check_connection` method detects some errors and tries to re-open the
    connections.

    https://github.com/fractal-analytics-platform/fractal-server/issues/2019
    """

    # Initialize new fractal_ssh object
    fractal_ssh = FractalSSHList().get(
        host=slurmlogin_ip,
        user="fractal",
        key_path=ssh_keys["private"],
    )

    # Prepare local/remote files
    local_file = (tmp777_path / "local").as_posix()
    with open(local_file, "w") as f:
        f.write("hi there\n")
    remote_file_1 = (tmp777_path / "remote_1").as_posix()
    remote_file_2 = (tmp777_path / "remote_2").as_posix()

    # Open connection and run an SFTP command
    fractal_ssh.check_connection()
    fractal_ssh.send_file(local=local_file, remote=remote_file_1)

    # Check sockets are open
    debug(fractal_ssh._sftp_unsafe().sock)
    assert not fractal_ssh._sftp_unsafe().sock.closed

    # Manually close sockets
    fractal_ssh._sftp_unsafe().sock.close()
    fractal_ssh._sftp_unsafe().sock.closed = True

    # Check sockets are closed
    debug(fractal_ssh._sftp_unsafe().sock)
    assert fractal_ssh._sftp_unsafe().sock.closed

    # Running an SFTP command now fails with an OSError
    fractal_ssh.send_file(local=local_file, remote=remote_file_2)
    assert Path(remote_file_2).exists()

    # `check_connection` does its best to restore a corrupt connection
    fractal_ssh.check_connection()

    # Check sockets are open
    debug(fractal_ssh._sftp_unsafe().sock)
    assert not fractal_ssh._sftp_unsafe().sock.closed

    # Successfully run a SFTP command
    fractal_ssh.send_file(local=local_file, remote=remote_file_2)
    assert fractal_ssh.remote_exists(remote_file_2)

    fractal_ssh.close()


@pytest.mark.container
def test_read_remote_text_file(
    fractal_ssh: FractalSSH,
    tmp777_path: Path,
):
    filepath = (tmp777_path / "file").as_posix()
    CONTENTS = "somethin\nelse"

    # Failure
    with pytest.raises(FileNotFoundError):
        fractal_ssh.read_remote_text_file(filepath)

    # Success
    with open(filepath, "w") as f:
        f.write(CONTENTS)
    assert fractal_ssh.read_remote_text_file(filepath) == CONTENTS
