from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import TimeoutError


logger = set_logger(__file__)


def test_unit_concurrent_run(fractal_ssh: FractalSSH):
    def _run_sleep(label: str, lock_timeout: float):
        logger.info(f"Start running with {label=}")
        fractal_ssh.run_command_over_ssh(
            cmd="sleep 1", lock_timeout=lock_timeout
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Try running two concurrent runs, with long lock timeout
        results_iterator = executor.map(_run_sleep, ["A", "B"], [2, 2])
        list(results_iterator)
        # Try running two concurrent runs and fail, due to short lock timeout
        results_iterator = executor.map(_run_sleep, ["C", "D"], [0.1, 0.1])
        with pytest.raises(TimeoutError) as e:
            list(results_iterator)
        assert "Failed to acquire lock" in str(e.value)


def test_unit_concurrent_put(fractal_ssh: FractalSSH, tmp_path: Path):
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
        with pytest.raises(TimeoutError) as e:
            list(results_iterator)
        assert "Failed to acquire lock" in str(e.value)
