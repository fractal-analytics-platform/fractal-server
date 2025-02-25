import json
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm_sudo.executor import (
    FractalSlurmSudoExecutor,
)
from fractal_server.app.runner.executors.slurm_sudo.executor import SlurmJob
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

    with FractalSlurmSudoExecutor(
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

    with FractalSlurmSudoExecutor(
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

    with FractalSlurmSudoExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=tmp_path / "remote_job_dir1",
        slurm_user="TEST",
        slurm_poll_interval=1,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            executor.wait_thread.wait(filenames=("some", "thing"), jobid=1)
        debug(exc_info.value)

    with FractalSlurmSudoExecutor(
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


@pytest.mark.skip(reason="NOT YET READY TO BE TESTED - FIXME")
def test_check_remote_runner_python_interpreter(
    monkeypatch,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/remote/python")

    with pytest.raises(
        RuntimeError, match="No such file or directory: '/remote/python'"
    ):
        FractalSlurmSudoExecutor(
            slurm_user="test_user",
            workflow_dir_local=Path("/local/workflow"),
            workflow_dir_remote=Path("/remote/workflow"),
        )

    def mock_subprocess_run_or_raise(cmd):
        class MockCompletedProcess(object):
            stdout: str = json.dumps({"fractal_server": "9.9.9"})

        return MockCompletedProcess()

    monkeypatch.setattr(
        (
            "fractal_server.app.runner.executors.slurm.sudo.executor"
            "._subprocess_run_or_raise"
        ),
        mock_subprocess_run_or_raise,
    )

    with pytest.raises(RuntimeError, match="Fractal-server version mismatch"):
        FractalSlurmSudoExecutor(
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


def test_FractalSlurmExecutor_init(
    tmp_path,
    override_settings_factory,
):

    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON=None)

    with pytest.raises(
        RuntimeError,
        match="Missing attribute FractalSlurmExecutor.slurm_user",
    ):
        with FractalSlurmSudoExecutor(
            slurm_user=None,
            workflow_dir_local=tmp_path / "job_dir1",
            workflow_dir_remote=tmp_path / "remote_job_dir1",
        ):
            pass

    with pytest.raises(
        RuntimeError,
        match="Missing attribute FractalSlurmExecutor.slurm_user",
    ):
        with FractalSlurmSudoExecutor(
            slurm_user="",
            workflow_dir_local=tmp_path / "job_dir1",
            workflow_dir_remote=tmp_path / "remote_job_dir1",
        ):
            pass

    with pytest.raises(
        RuntimeError,
        match="SLURM account must be set via the request body",
    ):
        with FractalSlurmSudoExecutor(
            slurm_user="something",
            workflow_dir_local=tmp_path / "job_dir1",
            workflow_dir_remote=tmp_path / "remote_job_dir1",
            common_script_lines=["#SBATCH --account=myaccount"],
        ):
            pass
