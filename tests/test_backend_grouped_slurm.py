import time

import pytest
from devtools import debug

from fractal_server.app.runner._grouped_slurm.executor import (
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
