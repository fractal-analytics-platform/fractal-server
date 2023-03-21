import pytest
from devtools import debug

from fractal_server.app.runner._grouped_slurm.executor import (
    FractalSlurmExecutor,
)  # noqa


@pytest.mark.skip()
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
