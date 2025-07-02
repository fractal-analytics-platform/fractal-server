import sys
from pathlib import Path

import pytest

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm_common.base_slurm_runner import (  # noqa
    BaseSlurmRunner,
)
from fractal_server.app.runner.executors.slurm_common.slurm_job_task_models import (  # noqa
    SlurmJob,
)


class MockBaseSlurmRunner(BaseSlurmRunner):
    def _mkdir_local_folder(self, *args):
        pass

    def _mkdir_remote_folder(self, *args):
        pass


async def test_validate_slurm_jobs_workdirs(tmp_path: Path):
    jobs_ok = [
        SlurmJob(
            prefix="prefix1",
            workdir_local=(tmp_path / "server/task_A"),
            workdir_remote=(tmp_path / "user/task_A"),
            tasks=[],
        ),
        SlurmJob(
            prefix="prefix2",
            workdir_local=(tmp_path / "server/task_A"),
            workdir_remote=(tmp_path / "user/task_A"),
            tasks=[],
        ),
    ]

    jobs_bad_1 = [
        SlurmJob(
            prefix="prefix1",
            workdir_local=(tmp_path / "server/task_A"),
            workdir_remote=(tmp_path / "user/task_A"),
            tasks=[],
        ),
        SlurmJob(
            prefix="prefix2",
            workdir_local=(tmp_path / "server/task_B"),
            workdir_remote=(tmp_path / "user/task_A"),
            tasks=[],
        ),
    ]

    jobs_bad_2 = [
        SlurmJob(
            prefix="prefix1",
            workdir_local=(tmp_path / "server/task_A"),
            workdir_remote=(tmp_path / "user/task_A"),
            tasks=[],
        ),
        SlurmJob(
            prefix="prefix2",
            workdir_local=(tmp_path / "server/task_A"),
            workdir_remote=(tmp_path / "user/task_B"),
            tasks=[],
        ),
    ]

    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        runner.validate_slurm_jobs_workdirs(jobs_ok)
        with pytest.raises(ValueError, match="Non-unique"):
            runner.validate_slurm_jobs_workdirs(jobs_bad_1)
        with pytest.raises(ValueError, match="Non-unique"):
            runner.validate_slurm_jobs_workdirs(jobs_bad_2)


async def test_check_no_active_jobs(tmp_path: Path):

    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        # Success
        runner._check_no_active_jobs()

        # Failure
        runner.jobs = {0: "fake"}
        with pytest.raises(JobExecutionError, match="jobs must be empty"):
            runner._check_no_active_jobs()


async def test_not_implemented_errors(tmp_path: Path):
    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        with pytest.raises(NotImplementedError):
            runner._run_remote_cmd(cmd="ls")

        with pytest.raises(NotImplementedError):
            runner.run_squeue(job_ids=[])

        with pytest.raises(NotImplementedError):
            runner._fetch_artifacts(finished_slurm_jobs=[])


async def test_recoverable_squeue_error(tmp_path: Path):

    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        recoverable_msg = """
Encountered a bad command exit code!
Command: \"squeue --noheader --format='%i %T' --states=all --jobs=111,222,333\"
Exit code: 1
Stdout:
Stderr:
slurm_load_jobs error: Socket timed out on send/recv operation
.
"""
        non_recoverable_msg = """
Encountered a bad command exit code!
Command: \"ls --invalid\"
Exit code: 2
Stdout:
Stderr:
ls: unrecognized option '--invalid'
Try 'ls --help' for more information.
.
"""
        assert runner._is_squeue_error_recoverable(
            RuntimeError(recoverable_msg)
        )
        assert not runner._is_squeue_error_recoverable(
            RuntimeError(non_recoverable_msg)
        )
