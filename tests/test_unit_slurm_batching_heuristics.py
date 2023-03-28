import pytest
from devtools import debug

from fractal_server.app.runner._grouped_slurm._batching_heuristics import (
    heuristics,
)


@pytest.mark.parametrize(
    "n_ftasks_per_script,n_parallel_ftasks_per_script",
    [
        (None, None),  # use the heuristics
        (20, 1),  # ask for 20-tasks job with no parallelism
    ],
)
@pytest.mark.parametrize(
    "n_ftasks_tot,cpus_per_task,mem_per_task",  # task properties
    [
        (1, 1, 7000),  # yokogawa-to-zarr task for 1 well
        (10, 1, 7000),  # yokogawa-to-zarr task for 10 wells
        (400, 1, 7000),  # yokogawa-to-zarr task for 400 wells
        (1, 16, 63000),  # cellpose task for 1 well
        (10, 16, 63000),  # cellpose task for 10 wells
        (400, 16, 63000),  # cellpose task for 400 wells
    ],
)
@pytest.mark.parametrize(
    "max_cpus_per_job,max_mem_per_job,max_num_jobs",  # cluster configuration  # noqa
    [
        (16, 64000, 1000),  # nodes with 16 CPUs, 64G memory, max 1000 jobs
        (
            192,
            805000,
            100,
        ),  # nodes with 192 CPUs, 805G memory, max 100 jobs  # noqa
    ],
)
def test_heuristics(
    n_ftasks_per_script: int | None,
    n_parallel_ftasks_per_script: int | None,
    n_ftasks_tot: int,
    cpus_per_task: int,
    max_cpus_per_job: int,
    mem_per_task: int,
    max_mem_per_job: int,
    max_num_jobs: int,
):
    debug(n_ftasks_per_script)
    debug(n_parallel_ftasks_per_script)

    target_cpus_per_job = int(max_cpus_per_job * 0.75)
    target_mem_per_job = int(max_mem_per_job * 0.75)
    target_num_jobs = int(max_num_jobs * 0.75)

    n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(
        n_ftasks_per_script=n_ftasks_per_script,
        n_parallel_ftasks_per_script=n_parallel_ftasks_per_script,
        n_ftasks_tot=n_ftasks_tot,
        cpus_per_task=cpus_per_task,
        target_cpus_per_job=target_cpus_per_job,
        max_cpus_per_job=max_cpus_per_job,
        mem_per_task=mem_per_task,
        target_mem_per_job=target_mem_per_job,
        max_mem_per_job=max_mem_per_job,
        target_num_jobs=target_num_jobs,
        max_num_jobs=max_num_jobs,
    )
    debug(n_ftasks_per_script)
    debug(n_parallel_ftasks_per_script)
