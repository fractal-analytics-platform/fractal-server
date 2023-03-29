import pytest
from devtools import debug

from fractal_server.app.runner._grouped_slurm._batching_heuristics import (
    heuristics,
)


clusters = [
    dict(
        target_cpus_per_job=8,
        max_cpus_per_job=16,
        target_mem_per_job=31000,
        max_mem_per_job=62000,
        target_num_jobs=100,
        max_num_jobs=200,
    ),
    dict(
        target_cpus_per_job=48,
        max_cpus_per_job=96,
        target_mem_per_job=62000,
        max_mem_per_job=125000,
        target_num_jobs=5,
        max_num_jobs=10,
    ),
]


@pytest.mark.parametrize("n_ftasks_tot", [1, 10, 40, 96, 400])
@pytest.mark.parametrize(
    "task_requirements", [(1, "4G"), (16, "62G"), (4, "16G")]
)
@pytest.mark.parametrize("cluster", clusters)
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
