import sys
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_common.base_slurm_runner import (  # noqa
    BaseSlurmRunner,
)
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (  # noqa
    SlurmJob,
)


class MockBaseSlurmRunner(BaseSlurmRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, poll_interval=0)

    def _mkdir_local_folder(self, folder: str) -> None:
        pass

    def _mkdir_remote_folder(self, folder: str) -> None:
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
        user_cache_dir=(tmp_path / "cache").as_posix(),
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
        user_cache_dir=(tmp_path / "cache").as_posix(),
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        # Success
        runner._check_no_active_jobs()

        # Failure
        runner.jobs = {
            "123": SlurmJob(
                slurm_job_id="123",
                prefix="fake",
                workdir_local=tmp_path / "fake",
                workdir_remote=tmp_path / "remote/fake",
                tasks=[],
            )
        }
        with pytest.raises(JobExecutionError, match="jobs must be empty"):
            runner._check_no_active_jobs()


async def test_not_implemented_errors(tmp_path: Path):
    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        user_cache_dir=(tmp_path / "cache").as_posix(),
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        with pytest.raises(NotImplementedError):
            runner._run_remote_cmd(cmd="ls")

        with pytest.raises(NotImplementedError):
            runner.run_squeue(job_ids=[])

        with pytest.raises(NotImplementedError):
            runner._fetch_artifacts(finished_slurm_jobs=[])


async def test_get_finished_jobs(tmp_path: Path):
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

    def patched_run_squeue(*, job_ids: list[str], **kwargs):
        """
        This is a mock, so that we can easily cover several branches
        of `BaseSlurmRunner._get_finished_jobs`.

        Cases:
            * [1,2] -> generic failure
            * [1] -> recoverable error
            * [2], [3], or [4] -> non-recoverable error
            * else -> all COMPLETED
        """
        debug(f"Enter `patched_run_squeue({job_ids})`")
        if job_ids == ["1", "2"] or job_ids == ["3", "4"]:
            raise ValueError(f"Error for {job_ids=}.")
        elif job_ids == ["1"]:
            raise ValueError(recoverable_msg)
        elif job_ids in [["2"], ["3"], ["4"]]:
            raise ValueError(non_recoverable_msg)
        else:
            output = ""
            for job_id in job_ids:
                output = f"{output}\n{job_id} COMPLETED"
            return output

    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        user_cache_dir=(tmp_path / "cache").as_posix(),
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        runner.run_squeue = patched_run_squeue

        assert runner._is_squeue_error_recoverable(
            RuntimeError(recoverable_msg)
        )
        assert not runner._is_squeue_error_recoverable(
            RuntimeError(non_recoverable_msg)
        )

        finished_jobs = runner._get_finished_jobs(job_ids=["1", "2"])
        debug(finished_jobs)
        assert finished_jobs == {"2"}

        finished_jobs = runner._get_finished_jobs(job_ids=["3", "4"])
        assert finished_jobs == {"3", "4"}


async def test_extract_slurm_error_and_set_executor_error_log(tmp_path: Path):
    job1 = SlurmJob(
        slurm_job_id="123",
        prefix="job1",
        workdir_local=tmp_path / "job1",
        workdir_remote=tmp_path / "remote/job1",
        tasks=[],
    )
    job2 = SlurmJob(
        slurm_job_id="456",
        prefix="job2",
        workdir_local=tmp_path / "job2",
        workdir_remote=tmp_path / "remote/job2",
        tasks=[],
    )
    job3 = SlurmJob(
        slurm_job_id="789",
        prefix="job3",
        workdir_local=tmp_path / "job3",
        workdir_remote=tmp_path / "remote/job3",
        tasks=[],
    )
    job4 = SlurmJob(
        slurm_job_id="711",
        prefix="job4",
        workdir_local=tmp_path / "job4",
        workdir_remote=tmp_path / "remote/job4",
        tasks=[],
    )
    job5 = SlurmJob(
        slurm_job_id="7123",
        prefix="job5",
        workdir_local=tmp_path / "job5",
        workdir_remote=tmp_path / "remote/job5",
        tasks=[],
    )

    for job in [job1, job2, job3, job4, job5]:
        job.workdir_local.mkdir(parents=True, exist_ok=True)

    err_msg = (
        "sbatch: error: Unable to allocate resources: "
        "Invalid account or account/partition combination specified\n"
    )

    err_msg_to_be_skipped = (
        "srun: Job 23547409 step creation temporarily disabled, retrying ..\n"
        "srun: Job 23547409 step creation temporarily disabled, retrying ..\n"
        "srun: Job 23547409 step creation temporarily disabled, retrying ..\n"
        "   \n"
        "srun: Job 23547409 step creation temporarily disabled, retrying ..\n"
        "srun: Job 23547409 step creation still disabled, retrying ..\n"
        "srun: Job 23547409 step creation still disabled, retrying ..\n"
        "   \n"
        "srun: Job 23547409 step creation still disabled, retrying ..\n"
        "srun: Job 23547409 step creation still disabled, retrying ..\n"
        "srun: Step created for StepId=23547409.1\n"
        "srun: Step created for StepId=23547409.2\n"
        "srun: Step created for StepId=23547409.3\n"
        "srun: Step created for StepId=23547409.4\n"
        "          \n"
    )

    # Create stderr files with different content
    # Job 1: Has SLURM error
    stderr1_path = job1.slurm_stderr_local_path
    stderr1_path.write_text(err_msg)

    # Job 2: Has empty stderr file
    stderr2_path = job2.slurm_stderr_local_path
    stderr2_path.write_text("")

    # Job 3: No stderr file (doesn't exist)

    # Job 4: Exception (it is a directory)
    job4.slurm_stderr_local_path.mkdir()

    # Job 5: Only has lines to be skipped
    stderr5_path = job5.slurm_stderr_local_path
    stderr5_path.write_text(err_msg_to_be_skipped)

    with MockBaseSlurmRunner(
        root_dir_local=tmp_path / "server",
        root_dir_remote=tmp_path / "user",
        user_cache_dir=(tmp_path / "cache").as_posix(),
        slurm_runner_type="sudo",
        python_worker_interpreter=sys.executable,
    ) as runner:
        # Test _extract_slurm_error for individual jobs
        error1 = runner._extract_slurm_error(job1)
        assert error1.strip() == err_msg.strip()

        error2 = runner._extract_slurm_error(job2)
        assert error2 is None  # Empty file

        error3 = runner._extract_slurm_error(job3)
        assert error3 is None  # File doesn't exist

        error4 = runner._extract_slurm_error(job4)
        assert error4 is None  # Exception while reading

        error5 = runner._extract_slurm_error(job5)
        assert error5 is None  # All lines to be skipped

        # Test _set_executor_error_log with multiple jobs
        assert runner.executor_error_log is None

        # Set error log from jobs - should capture first error (job1)
        runner._set_executor_error_log([job1, job2, job3])
        assert runner.executor_error_log.strip() == err_msg.strip()

        # Reset and test with different order
        runner.executor_error_log = None
        runner._set_executor_error_log([job3, job2, job1])
        assert runner.executor_error_log.strip() == err_msg.strip()

        # Test that once set, it doesn't change
        runner._set_executor_error_log([job1, job2, job3])
        assert runner.executor_error_log.strip() == err_msg.strip()

        # Test with no errors
        runner.executor_error_log = None
        runner._set_executor_error_log([job2, job3])
        assert runner.executor_error_log is None
