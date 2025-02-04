import json
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm.sudo.executor import (
    FractalSlurmExecutor,
)
from fractal_server.app.runner.executors.slurm.sudo.executor import SlurmJob
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2._aux_runner import get_default_task_files


def test_slurm_sudo_executor_shutdown_before_job_submission(
    tmp_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    """
    Verify the behavior when shutdown is called before any job has started.
    """

    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON=None)

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
            executor.wait_thread.wait(filenames=("some", "thing"), jobid=1)
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


def test_check_remote_runner_python_interpreter(
    monkeypatch, override_settings_factory
):
    remote_version = "1.0.0"
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/remote/python")

    def mock_subprocess_run_or_raise(cmd):
        class MockCompletedProcess(object):
            stdout: str = json.dumps({"fractal_server": remote_version})

        return MockCompletedProcess()

    with pytest.raises(
        RuntimeError, match="No such file or directory: '/remote/python'"
    ):
        FractalSlurmExecutor(
            slurm_user="test_user",
            workflow_dir_local=Path("/local/workflow"),
            workflow_dir_remote=Path("/remote/workflow"),
        )

    monkeypatch.setattr(
        (
            "fractal_server.app.runner.executors.slurm.sudo.executor"
            "._subprocess_run_or_raise"
        ),
        mock_subprocess_run_or_raise,
    )

    with pytest.raises(RuntimeError, match="Fractal-server version mismatch"):
        FractalSlurmExecutor(
            slurm_user="test_user",
            workflow_dir_local=Path("/local/workflow"),
            workflow_dir_remote=Path("/remote/workflow"),
        )


def test_SlurmJob():
    job = SlurmJob(
        single_task_submission=False,
        num_tasks_tot=2,
        wftask_file_prefixes=("0", "1"),
        slurm_config=get_default_slurm_config(),
        slurm_file_prefix="prefix",
    )
    assert job.wftask_file_prefixes == ("0", "1")

    job = SlurmJob(
        single_task_submission=False,
        num_tasks_tot=2,
        wftask_file_prefixes=None,
        slurm_config=get_default_slurm_config(),
        slurm_file_prefix="prefix",
    )
    assert job.wftask_file_prefixes == (
        "default_wftask_prefix",
        "default_wftask_prefix",
    )

    with pytest.raises(ValueError, match="Trying to initialize"):
        SlurmJob(
            single_task_submission=True,
            num_tasks_tot=2,
            wftask_file_prefixes=("0", "1"),
            slurm_config=get_default_slurm_config(),
            slurm_file_prefix="prefix",
        )
