import logging
from pathlib import Path

import pytest

from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.config import PixiSLURMConfig
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    FRACTAL_SQUEUE_ERROR_STATE,
)
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import _log_change_of_job_state
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import _read_file_if_exists
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import _run_squeue
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    _verify_success_file_exists,
)
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    run_script_on_remote_slurm,
)


def test_log_change_of_job_state(caplog):
    LOGGER_NAME = "my-logger"
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    _log_change_of_job_state(
        old_state=None,
        new_state="new_state",
        logger_name=LOGGER_NAME,
    )
    assert "state changed" in caplog.text


def test_run_squeue_failure():
    state = _run_squeue(
        fractal_ssh=FractalSSH(connection=None),
        squeue_cmd="fake",
        logger_name="my-logger",
    )
    assert state == FRACTAL_SQUEUE_ERROR_STATE


@pytest.mark.container
@pytest.mark.ssh
def test_verify_success_file_exists(
    fractal_ssh: FractalSSH,
    tmp777_path: Path,
):
    # Stderr file missing
    stderr_remote = (tmp777_path / "stderr").as_posix()
    with pytest.raises(RuntimeError, match="missing"):
        _verify_success_file_exists(
            fractal_ssh=fractal_ssh,
            success_file_remote="/missing-success-file",
            logger_name="my-logger",
            stderr_remote=stderr_remote,
        )
    assert (
        _read_file_if_exists(
            fractal_ssh=fractal_ssh,
            path=stderr_remote,
        )
        == ""
    )

    # Stderr file exists
    Path(stderr_remote).touch()
    with pytest.raises(RuntimeError, match="missing"):
        _verify_success_file_exists(
            fractal_ssh=fractal_ssh,
            success_file_remote="/missing-success-file",
            logger_name="my-logger",
            stderr_remote=stderr_remote,
        )


def test_sbatch_failure(
    tmp777_path: Path,
    monkeypatch,
):
    class MockFractalSSH(FractalSSH):
        def write_remote_file(self, *args, **kwargs):
            pass

        def run_command(self, cmd, *args, **kwargs):
            raise ValueError(f"Fake failure of {cmd}")

    import fractal_server.tasks.v2.ssh._pixi_slurm_ssh

    class MockActivity:
        log: str = "log"

    def _get_MockActivity(*args, **kwargs) -> MockActivity:
        return MockActivity()

    monkeypatch.setattr(
        fractal_server.tasks.v2.ssh._pixi_slurm_ssh,
        "add_commit_refresh",
        _get_MockActivity,
    )

    script_path = (tmp777_path / "script.sh").as_posix()
    log_file_path = tmp777_path / "logs"
    log_file_path.touch()

    with pytest.raises(ValueError, match="sbatch"):
        run_script_on_remote_slurm(
            script_paths=[script_path],
            slurm_config=PixiSLURMConfig(
                mem="1G", cpus=1, partition="main", time="10"
            ).model_dump(),
            fractal_ssh=MockFractalSSH(connection=None),
            logger_name="my-logger",
            log_file_path=log_file_path,
            prefix="prefix",
            activity=MockActivity(),
            db=None,
            poll_interval=1,
        )

    # Repeat, with different memory configuration
    with pytest.raises(ValueError, match="sbatch"):
        run_script_on_remote_slurm(
            script_paths=[script_path],
            slurm_config=PixiSLURMConfig(
                mem_per_cpu="1G", cpus=1, partition="main", time="10"
            ).model_dump(),
            fractal_ssh=MockFractalSSH(connection=None),
            logger_name="my-logger",
            log_file_path=log_file_path,
            prefix="prefix",
            activity=MockActivity(),
            db=None,
            poll_interval=1,
        )
