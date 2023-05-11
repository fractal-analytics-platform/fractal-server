import shlex
import time
from concurrent.futures import Executor
from typing import Callable

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue
from .fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from fractal_server.app.runner._slurm.executor import (
    FractalSlurmExecutor,
)  # noqa
from fractal_server.app.runner.common import JobExecutionError
from fractal_server.app.runner.common import TaskExecutionError


def test_slurm_executor_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_slurm_executor_submit_with_exception(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    def raise_ValueError():
        raise ValueError

    with pytest.raises(TaskExecutionError) as e:
        with FractalSlurmExecutor(
            slurm_user=monkey_slurm_user,
            working_dir=tmp777_path,
            working_dir_user=tmp777_path,
            slurm_poll_interval=2,
            keep_pickle_files=True,
        ) as executor:
            fut = executor.submit(raise_ValueError)
            debug(fut.result())
    debug(e.value)


def test_slurm_executor_map(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
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
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
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

    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
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
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
    slurm_working_folders,
):
    """
    Same as test_slurm_executor, but with two folders:
    * server_working_dir is owned by the server user and has 755 permissions
    * user_working_dir is owned the user and had default permissions
    """

    server_working_dir, user_working_dir = slurm_working_folders

    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=server_working_dir,
        working_dir_user=user_working_dir,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_slurm_executor_submit_and_scancel(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
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

    server_working_dir, user_working_dir = slurm_working_folders

    with pytest.raises(JobExecutionError) as e:
        with FractalSlurmExecutor(
            slurm_user=monkey_slurm_user,
            working_dir=server_working_dir,
            working_dir_user=user_working_dir,
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
                slurm_user=monkey_slurm_user, show_squeue=True
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
            working_dir=tmp_path, working_dir_user=tmp777_path
        )
    with pytest.raises(RuntimeError):
        FractalSlurmExecutor(slurm_user=None)


def submit_and_ignore_exceptions(
    executor: Executor, fun: Callable, *args, **kwargs
):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(f"Ignored exception: {str(e)}")


def test_submit_pre_command(fake_process, tmp_path, cfut_jobs_finished):
    """
    GIVEN a FractalSlurmExecutor
    WHEN it is initialised with a slurm_user
    THEN the sbatch call contains the sudo pre-command
    """
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])
    fake_process.register(["squeue", fake_process.any()])

    slurm_user = "some-fake-user"

    with FractalSlurmExecutor(
        slurm_user=slurm_user,
        working_dir=tmp_path,
        working_dir_user=tmp_path,
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
