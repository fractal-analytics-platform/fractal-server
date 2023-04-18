import time

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue
from fractal_server.app.runner._slurm.executor import (
    FractalSlurmExecutor,
)  # noqa
from fractal_server.app.runner.common import JobExecutionError


def test_slurm_executor_shutdown_during_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    def fun_sleep():
        time.sleep(5)
        return 42

    executor = FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    )
    res = executor.submit(fun_sleep)
    debug(res)
    debug(run_squeue())
    executor.shutdown()
    debug(res)
    debug(run_squeue())
    with pytest.raises(JobExecutionError):
        _ = res.result()


@pytest.mark.skip("map is blocking, we need an indirect shutdown call")
def test_slurm_executor_shutdown_during_map(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    def fun_sleep(dummy):
        time.sleep(10)
        return 42

    executor = FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    )
    res = executor.map(fun_sleep, range(100))
    debug(res)
    debug(run_squeue())
    executor.shutdown()
    debug(res)
    debug(run_squeue())
