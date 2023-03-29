import math

import pytest

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

print("")
cols = (
    "Cluster   | "
    "cpus/task | "
    "mem/task | "
    "| "
    "#jobs | "
    "tasks/script | "
    "parallel_tasks/script | "
    "Parallelism |"
)
print(cols)


@pytest.mark.parametrize("n_ftasks_tot", [1, 10, 40, 96, 400])
@pytest.mark.parametrize(
    "task_requirements", [(1, 4000), (16, 62000), (4, 16000)]
)
@pytest.mark.parametrize("cluster", clusters)
def test_heuristics(
    n_ftasks_tot: int,
    task_requirements: tuple[int, int],
    cluster: tuple[dict[str, int]],
):
    cpus_per_task, mem_per_task = task_requirements[:]
    target_cpus_per_job = cluster["target_cpus_per_job"]
    max_cpus_per_job = cluster["max_cpus_per_job"]
    target_mem_per_job = cluster["target_mem_per_job"]
    max_mem_per_job = cluster["max_mem_per_job"]
    target_num_jobs = cluster["target_num_jobs"]
    max_num_jobs = cluster["max_num_jobs"]

    n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(
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
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    parallelism = n_parallel_ftasks_per_script / n_ftasks_per_script
    cluster_index = clusters.index(cluster)
    cluster_name = f"cluster_{cluster_index}"

    output = (
        f"{cluster_name} | "
        f"{cpus_per_task:9d} | "
        f"{mem_per_task:8d} | "
        "| "
        f"{num_jobs:5d} | "
        f"{n_ftasks_per_script:12d} | "
        f"{n_parallel_ftasks_per_script:21d} | "
        f"{parallelism:11.3f} |"
    )

    print()
    print(output)
    print()
