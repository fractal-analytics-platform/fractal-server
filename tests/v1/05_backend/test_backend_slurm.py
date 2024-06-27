import logging
import os
import shlex
import time
from concurrent.futures import Executor
from typing import Callable

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm._subprocess_run_as_user import (
    _mkdir_as_user,
)
from fractal_server.app.runner.executors.slurm.executor import (
    FractalSlurmExecutor,
)  # noqa
from tests.fixtures_slurm import run_squeue
from tests.fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from tests.fixtures_slurm import SLURM_USER


class MockFractalSlurmExecutor(FractalSlurmExecutor):
    def __init__(self, *args, **kwargs):
        """
        When running from outside Fractal runner, task-specific subfolders
        must be created by hand.
        """
        super().__init__(*args, **kwargs)
        task_files = self.get_default_task_files()

        # Server-side subfolder
        server_side_subfolder = (
            task_files.workflow_dir_local / task_files.subfolder_name
        )
        umask = os.umask(0)
        logging.info(f"Now creating {server_side_subfolder.as_posix()}")
        server_side_subfolder.mkdir()
        os.umask(umask)

        # User-side subfolder
        if self.workflow_dir_local != self.workflow_dir_remote:
            logging.info(
                f"Now creating {task_files.remote_subfolder.as_posix()}, "
                "as user."
            )
            _mkdir_as_user(
                folder=task_files.remote_subfolder.as_posix(),
                user=self.slurm_user,
            )


def test_slurm_executor_submit_missing_subfolder(
    monkey_slurm,
    tmp777_path,
):
    """
    If the task-specific subfolder is missing, the executor should
    raise a FileNotFoundError.

    Note that in this test we don't use MockFractalSlurmExecutor.
    """
    with pytest.raises(FileNotFoundError):
        with FractalSlurmExecutor(
            slurm_user=SLURM_USER,
            workflow_dir_local=tmp777_path,
            workflow_dir_remote=tmp777_path,
            slurm_poll_interval=2,
            keep_pickle_files=True,
        ) as executor:
            executor.submit(lambda: 42)


def test_slurm_executor_submit(
    monkey_slurm,
    tmp777_path,
):
    with MockFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:

        fut1 = executor.submit(lambda: 1)
        fut2 = executor.submit(lambda: 2)
        assert fut2.result() == 2
        assert fut1.result() == 1


def test_slurm_executor_submit_with_exception(
    monkey_slurm,
    tmp777_path,
):
    def raise_ValueError():
        raise ValueError

    with pytest.raises(TaskExecutionError) as e:
        with MockFractalSlurmExecutor(
            slurm_user=SLURM_USER,
            workflow_dir_local=tmp777_path,
            workflow_dir_remote=tmp777_path,
            slurm_poll_interval=2,
            keep_pickle_files=True,
        ) as executor:
            fut = executor.submit(raise_ValueError)
            debug(fut.result())
    debug(e.value)


def test_slurm_executor_map(
    monkey_slurm,
    tmp777_path,
):
    with MockFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:
        inputs = list(range(10))
        result_generator = executor.map(lambda x: 2 * x, inputs)
        results = list(result_generator)
        debug(results)
        assert results == [2 * x for x in inputs]


@pytest.mark.parametrize("early_late", ["early", "late"])
def test_slurm_executor_map_with_exception(
    early_late,
    monkey_slurm,
    tmp777_path,
):

    """
    NOTE: Tasks submitted to FractalSlurmExecutor via fractal-server always
    return either JobExecutionError or TaskExecutionError, while for functions
    submitted directly to the executor (and raising arbitrary exceptions like a
    ValueError) this is not true. Depending on the way the error is raised, in
    this test, the resulting error could be a JobExecutionError or
    TaskExecutionError; we accept both of them, here.
    """

    debug(early_late)

    def _raise(n: int):
        if n == 1:
            if early_late == "late":
                time.sleep(1.5)
            raise ValueError
        else:
            if early_late == "early":
                time.sleep(1.5)
            return n

    with MockFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
        slurm_poll_interval=1,
        keep_pickle_files=True,
    ) as executor:
        try:
            result_generator = executor.map(_raise, range(10))
            for result in result_generator:
                debug(f"While looping over results, I got to {result=}")
            raise RuntimeError(
                "If we reached this line, then the test is failed"
            )
        except Exception as e:
            debug(e)
            debug(vars(e))
            assert type(e) in [TaskExecutionError, JobExecutionError]


def test_slurm_executor_submit_separate_folders(
    monkey_slurm,
    tmp777_path,
    slurm_working_folders,
):
    """
    Same as test_slurm_executor, but with two folders:
    * workflow_dir_local is owned by the server user and has 755 permissions
    * workflow_dir_remote is owned the user and had default permissions
    """

    workflow_dir_local, workflow_dir_remote = slurm_working_folders

    with MockFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:
        res = executor.submit(lambda: 42)
        assert res.result() == 42


def test_slurm_executor_submit_and_scancel(
    monkey_slurm,
    tmp777_path,
    slurm_working_folders,
):
    """
    GIVEN a docker slurm cluster and a FractalSlurmExecutor executor
    WHEN a function is submitted to the executor (as a given user) and then the
         SLURM job is immediately canceled
    THEN the error is correctly captured
    """

    import time

    def wait_and_return():
        time.sleep(60)
        return 42

    workflow_dir_local, workflow_dir_remote = slurm_working_folders

    with pytest.raises(JobExecutionError) as e:
        with MockFractalSlurmExecutor(
            slurm_user=SLURM_USER,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            debug=True,
            keep_pickle_files=True,
            slurm_poll_interval=2,
        ) as executor:
            fut = executor.submit(wait_and_return)
            debug(fut)

            # Wait until the SLURM job goes from PENDING to RUNNING
            while True:
                squeue_output = run_squeue(
                    squeue_format="%i %u %T", header=False
                )
                debug(squeue_output)
                if "RUNNING" in squeue_output:
                    break
                time.sleep(1)

            # Scancel all jobs of the current SLURM user
            scancel_all_jobs_of_a_slurm_user(
                slurm_user=SLURM_USER, show_squeue=True
            )

            # Calling result() forces waiting for the result, which in this
            # test raises an exception
            fut.result()

    debug(str(e.type))
    debug(str(e.value))
    debug(str(e.traceback))

    debug(e.value.assemble_error())

    assert "CANCELLED" in e.value.assemble_error()
    # Since we waited for the job to be RUNNING, both the SLURM stdout and
    # stderr files should exist
    assert "missing" not in e.value.assemble_error()


def test_missing_slurm_user(tmp_path, tmp777_path):
    with pytest.raises(TypeError):
        FractalSlurmExecutor(
            workflow_dir_local=tmp_path, workflow_dir_remote=tmp777_path
        )
    with pytest.raises(RuntimeError):
        FractalSlurmExecutor(
            slurm_user=None,
            workflow_dir_local=tmp_path,
            workflow_dir_remote=tmp777_path,
        )


def test_slurm_account_in_common_script_lines(tmp_path, tmp777_path):
    # No error
    FractalSlurmExecutor(
        slurm_user="slurm_user",
        workflow_dir_local=tmp_path,
        workflow_dir_remote=tmp777_path,
        common_script_lines=["#SBATCH --partition=something"],
    )

    # Error
    with pytest.raises(RuntimeError) as e:
        FractalSlurmExecutor(
            slurm_user="slurm_user",
            workflow_dir_local=tmp_path,
            workflow_dir_remote=tmp777_path,
            common_script_lines=["#SBATCH --account=something"],
        )
    debug(str(e.value))
    assert "SLURM account" in str(e.value)


def submit_and_ignore_exceptions(
    executor: Executor, fun: Callable, *args, **kwargs
):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(f"Ignored exception: {str(e)}")


def test_submit_pre_command(fake_process, tmp_path):
    """
    GIVEN a FractalSlurmExecutor
    WHEN it is initialised with a slurm_user
    THEN the sbatch call contains the sudo pre-command
    """
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])
    fake_process.register(["squeue", fake_process.any()])

    slurm_user = "some-fake-user"

    with MockFractalSlurmExecutor(
        slurm_user=slurm_user,
        workflow_dir_local=tmp_path,
        workflow_dir_remote=tmp_path,
    ) as executor:
        submit_and_ignore_exceptions(executor, lambda: None)

    # Convert from deque to list, and apply shlex.join
    call_strings = [shlex.join(call) for call in fake_process.calls]
    debug(call_strings)

    # The first subprocess command in FractalSlurmExecutor (which fails, and
    # then stops the execution via submit_and_ignore_exceptions) is an `ls`
    # command to check that a certain folder exists. This will change if we
    # remove this check from FractalSlurmExecutor, or if another subprocess
    # command is called before the `ls` one.
    target = f"sudo --non-interactive -u {slurm_user} ls"
    debug([target in call for call in call_strings])
    assert any([target in call for call in call_strings])


def test_slurm_account_in_submit_script(tmp_path):
    """
    Check that submission script contains the right SLURM account.
    """
    slurm_user = "some-fake-user"
    SLURM_ACCOUNT = "FakeAccountForThisTest"

    # Without slurm_account argument
    tmp_path1 = tmp_path / "1"
    tmp_path1.mkdir()
    with MockFractalSlurmExecutor(
        slurm_user=slurm_user,
        workflow_dir_local=tmp_path1,
        workflow_dir_remote=tmp_path1,
    ) as executor:
        submit_and_ignore_exceptions(executor, lambda: None)

    submission_script_files = list(tmp_path1.glob("**/*.sbatch"))
    assert len(submission_script_files) == 1
    submission_script_file = submission_script_files[0]
    with submission_script_file.open("r") as f:
        lines = f.read().splitlines()
    debug(lines)
    try:
        invalid_line = next(
            line for line in lines if line.startswith("#SBATCH --account=")
        )
        raise RuntimeError(f"Line '{invalid_line}' cannot be there.")
    except StopIteration:
        pass

    # With slurm_account argument
    tmp_path2 = tmp_path / "2"
    tmp_path2.mkdir()
    with MockFractalSlurmExecutor(
        slurm_user=slurm_user,
        workflow_dir_local=tmp_path2,
        workflow_dir_remote=tmp_path2,
        slurm_account=SLURM_ACCOUNT,
    ) as executor:
        submit_and_ignore_exceptions(executor, lambda: None)

    submission_script_files = list(tmp_path2.glob("**/*.sbatch"))
    assert len(submission_script_files) == 1
    submission_script_file = submission_script_files[0]
    with submission_script_file.open("r") as f:
        lines = f.read().splitlines()
    debug(lines)
    assert f"#SBATCH --account={SLURM_ACCOUNT}" in lines
