from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fabric import Connection

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHTimeoutError

# from typing import Any
# from typing import Optional


logger = set_logger(__file__)


def test_acquire_lock():
    fake_fractal_ssh = FractalSSH(connection=Connection("localhost"))
    fake_fractal_ssh._lock.acquire(timeout=0)
    with pytest.raises(FractalSSHTimeoutError) as e:
        with fake_fractal_ssh.acquire_timeout(timeout=0.1):
            pass
    print(e)


def test_concurrent_run(fractal_ssh: FractalSSH):
    def _run_sleep(label: str, lock_timeout: float):
        logger.info(f"Start running with {label=} and {lock_timeout=}")
        fractal_ssh.run_command_over_ssh(
            cmd="sleep 1", lock_timeout=lock_timeout
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Try running two concurrent runs, with long lock timeout
        results_iterator = executor.map(_run_sleep, ["A", "B"], [2.0, 2.0])
        list(results_iterator)
        # Try running two concurrent runs and fail, due to short lock timeout
        res_it = executor.map(_run_sleep, ["C", "D"], [0.1, 0.1])
        with pytest.raises(FractalSSHTimeoutError) as e:
            list(res_it)
        print(e)


def test_concurrent_put(fractal_ssh: FractalSSH, tmp_path: Path):
    local_file = (tmp_path / "local").as_posix()
    with open(local_file, "w") as f:
        f.write("x" * 10_000)

    def _put_file(remote: str, lock_timeout: float):
        logger.info(f"Put into {remote=}.")
        fractal_ssh.put_over_ssh(
            local=local_file,
            remote=remote,
            lock_timeout=lock_timeout,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Try running two concurrent runs, with long lock timeout
        results_iterator = executor.map(
            _put_file, ["remote1", "remote2"], [1.0, 1.0]
        )
        list(results_iterator)
        # Try running two concurrent runs and fail, due to short lock timeout
        results_iterator = executor.map(
            _put_file, ["remote3", "remote4"], [0.0, 0.0]
        )
        with pytest.raises(FractalSSHTimeoutError) as e:
            list(results_iterator)
        assert "Failed to acquire lock" in str(e.value)


def test_unit_remove_folder_over_ssh_failures():
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


def test_remove_folder_over_ssh(tmp777_path, fractal_ssh: FractalSSH):
    assert fractal_ssh.is_connected

    # Define folder
    folder = (tmp777_path / "nested/folder").as_posix()

    # Check that folder does not exist
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(e.value)

    # Try to create folder, without parents options
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.mkdir(folder=folder, parents=False)
    print(e.value)

    # Create folder
    fractal_ssh.mkdir(folder=folder, parents=True)

    # Check that folder exists
    stdout = fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(stdout)
    print()

    # Remove folder
    fractal_ssh.remove_folder(folder=folder, safe_root="/tmp")

    # Check that folder does not exist
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(e.value)

    # Check that removing a missing folder fails
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.remove_folder(
            folder="/invalid/something",
            safe_root="/invalid",
        )
    print(e.value)


def test_run_command_fails(fractal_ssh: FractalSSH):
    """
    When the remotely-executed command fails, a RuntimeError is raised.
    """
    with pytest.raises(RuntimeError) as e:
        fractal_ssh.run_command_over_ssh(
            cmd="ls --invalid-option",
            max_attempts=1,
            base_interval=1.0,
            lock_timeout=1.0,
        )
    print(e.value)


# def test_run_command_second_attempt(fractal_ssh: FractalSSH, monkeypatch):
#     iteration = 0

#     def run(
#             self, *args, lock_timeout: Optional[float] = None, **kwargs
#         ) -> Any:

#             actual_lock_timeout = self.default_lock_timeout
#             if lock_timeout is not None:
#                 actual_lock_timeout = lock_timeout
#             with self.acquire_timeout(timeout=actual_lock_timeout):
#                 return self._connection.run(*args, **kwargs)


#     def _run_mock(*args, **kwargs):
#         if iteration == 0:
#             raise ValueError
#         else:
#             FractalSSH().run(*args, **kwargs)

#     monkeypatch.set

# fractal_ssh.run_command_over_ssh(
#     cmd="whoami", max_attempts=2, base_interval=0.1
#     )
