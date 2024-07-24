from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fabric import Connection
from paramiko.ssh_exception import NoValidConnectionsError

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHTimeoutError


logger = set_logger(__file__)


def test_acquire_lock():
    """
    Test that the lock cannot be acquired twice.
    """
    fake_fractal_ssh = FractalSSH(connection=Connection("localhost"))
    fake_fractal_ssh._lock.acquire(timeout=0)
    with pytest.raises(FractalSSHTimeoutError) as e:
        with fake_fractal_ssh.acquire_timeout(timeout=0.1):
            pass
    print(e)


def test_run_command(fractal_ssh: FractalSSH):
    """
    Basic working of `run_command` method.
    """

    # Successful remote execution
    stdout = fractal_ssh.run_command(
        cmd="whoami",
        max_attempts=1,
        base_interval=1.0,
        lock_timeout=1.0,
    )
    assert stdout.strip("\n") == "fractal"

    # When the remotely-executed command fails, a RuntimeError is raised.
    with pytest.raises(
        RuntimeError, match="Encountered a bad command exit code"
    ):
        fractal_ssh.run_command(
            cmd="ls --invalid-option",
            max_attempts=1,
            base_interval=1.0,
            lock_timeout=1.0,
        )


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


def test_run_command_retries(fractal_ssh: FractalSSH):
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

        def run(self, *args, **kwargs):
            if self.please_raise:
                # Set `please_raise=False`, so that next call will go through
                self.please_raise = False
                # Construct a NoValidConnectionsError. Note that we prepare an
                # `errors` attribute with the appropriate type, but with no
                # meaningful content
                errors = {("str", 1): ("str", 1, 1, 1)}
                raise NoValidConnectionsError(errors=errors)
            return super().run(*args, **kwargs)

    mocked_fractal_ssh = MockFractalSSH(connection=fractal_ssh._connection)

    # Call with max_attempts=1 fails
    with pytest.raises(RuntimeError, match="Reached last attempt"):
        mocked_fractal_ssh.run_command(cmd="whoami", max_attempts=1)

    # Call with max_attempts=2 goes through (note that we have to reset
    # `please_raise`)
    mocked_fractal_ssh.please_raise = True
    stdout = mocked_fractal_ssh.run_command(
        cmd="whoami", max_attempts=2, base_interval=0.1
    )
    assert stdout.strip() == "fractal"


def test_file_transfer(fractal_ssh: FractalSSH, tmp_path: Path):
    """
    Test basic working of `send_file` and `get` methods.
    """
    local_file_old = (tmp_path / "local_old").as_posix()
    local_file_new = (tmp_path / "local_new").as_posix()
    with open(local_file_old, "w") as f:
        f.write("hi there\n")

    # Send file
    fractal_ssh.send_file(local=local_file_old, remote="remote_file")

    # Get back file (note: we include the `lock_timeout` argument only
    # for coverage of the corresponding conditional branch)
    fractal_ssh.get(
        remote="remote_file", local=local_file_new, lock_timeout=1.0
    )
    assert Path(local_file_new).is_file()


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
    fractal_ssh.remove_folder(folder=folder, safe_root="/tmp")

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
    fake_fractal_ssh = FractalSSH(connection=Connection(host="localhost"))

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


def test_write_remote_file(fractal_ssh: FractalSSH, tmp777_path: Path):
    path = tmp777_path / "file"
    content = "this is what goes into the file"
    fractal_ssh.write_remote_file(path=path.as_posix(), content=content)
    assert path.exists()
    with path.open("r") as f:
        assert f.read() == content


def test_run_command_session_persistence(
    fractal_ssh: FractalSSH, tmp777_path: Path
):
    """
    FIXME add explanation
    """

    script1 = tmp777_path / "set_var.sh"
    with script1.open("w") as f:
        f.write("export MYVAR=123\necho $MYVAR")
    script2 = tmp777_path / "read_var.sh"
    with script2.open("w") as f:
        f.write("echo $MYVAR")

    stdout = fractal_ssh.run_command(cmd=f". {script1.as_posix()}")
    assert stdout.strip("\n") == "123"
    stdout = fractal_ssh.run_command(cmd=f". {script2.as_posix()}")
    assert stdout.strip("\n") == ""
