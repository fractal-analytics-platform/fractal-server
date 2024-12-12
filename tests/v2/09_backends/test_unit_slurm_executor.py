from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm.sudo.executor import (
    FractalSlurmExecutor,
)  # noqa
from fractal_server.logger import set_logger
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2._aux_runner import get_default_task_files

logger = set_logger(__file__)


def test_slurm_sudo_executor_shutdown_before_job_submission(
    tmp_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    """
    Verify the behavior when shutdown is called before any job has started.
    """

    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    with FractalSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=tmp_path / "remote_job_dir1",
        slurm_user="TEST",
        slurm_poll_interval=1,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            fut = executor.submit(
                lambda: 1,
                slurm_config=get_default_slurm_config(),
                task_files=get_default_task_files(
                    workflow_dir_local=executor.workflow_dir_local,
                    workflow_dir_remote=executor.workflow_dir_remote,
                ),
            )
            fut.result()
        debug(exc_info.value)

    with FractalSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=tmp_path / "remote_job_dir1",
        slurm_user="TEST",
        slurm_poll_interval=1,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            fut = executor.map(
                lambda x: 1,
                [1, 2, 3],
                slurm_config=get_default_slurm_config(),
                task_files=get_default_task_files(
                    workflow_dir_local=executor.workflow_dir_local,
                    workflow_dir_remote=executor.workflow_dir_remote,
                ),
            )
            fut.result()
        debug(exc_info.value)

    with FractalSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=tmp_path / "remote_job_dir1",
        slurm_user="TEST",
        slurm_poll_interval=1,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            executor.wait_thread.wait(filenames=("something"), jobid=1)
        debug(exc_info.value)

    with FractalSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=tmp_path / "remote_job_dir1",
        slurm_user="TEST",
        slurm_poll_interval=1,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            executor._submit_job(
                lambda x: x,
                slurm_file_prefix="test",
                task_files=get_default_task_files(
                    workflow_dir_local=executor.workflow_dir_local,
                    workflow_dir_remote=executor.workflow_dir_remote,
                ),
                slurm_config=get_default_slurm_config(),
            )
        debug(exc_info.value)
