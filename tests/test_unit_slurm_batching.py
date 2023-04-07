import math
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner._slurm._batching_heuristics import (
    heuristics,
)


clusters = [
    dict(
        target_cpus_per_job=8,
        max_cpus_per_job=16,
        target_mem_per_job=32000,
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


@pytest.fixture(scope="session")
def table_path(tmpdir_factory) -> Path:
    fn = tmpdir_factory.mktemp("table") / "table.txt"
    return fn


@pytest.mark.parametrize("n_ftasks_tot", [1, 10, 40, 96, 400])
@pytest.mark.parametrize(
    "task_requirements",
    [
        ("yoko2zarr", 1, 4000),
        ("napari-wf", 4, 16000),
        ("cellpose", 16, 61000),
    ],
)
@pytest.mark.parametrize("cluster", clusters)
def test_heuristics(
    n_ftasks_tot: int,
    task_requirements: tuple[str, int, int],
    cluster: tuple[dict[str, int]],
    table_path: Path,
):

    if not table_path.exists():
        cols = (
            "Cluster   | "
            "Task       | "
            "cpus/task | "
            "mem/task | "
            "#tasks || "
            "#jobs | "
            "max #tasks/script | "
            "max #parallel_tasks/script | "
            "Parallelism |\n"
        )
        debug(table_path)
        debug(cols)
        with table_path.open("w") as f:
            f.write(cols)

    task_label, cpus_per_task, mem_per_task = task_requirements[:]
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
    if n_parallel_ftasks_per_script == 0:
        debug(cluster)
        debug(task_requirements)
        debug(n_ftasks_tot)
        debug(n_ftasks_per_script)
        debug(n_parallel_ftasks_per_script)
        raise ValueError
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    parallelism = n_parallel_ftasks_per_script / n_ftasks_per_script
    cluster_index = clusters.index(cluster)
    cluster_name = f"cluster_{cluster_index}"

    output = (
        f"{cluster_name} | "
        f"{task_label:10s} | "
        f"{cpus_per_task:9d} | "
        f"{mem_per_task:8d} | "
        f"{n_ftasks_tot:6d} || "
        f"{num_jobs:5d} | "
        f"{n_ftasks_per_script:17d} | "
        f"{n_parallel_ftasks_per_script:26d} | "
        f"{parallelism:11.3f} |"
        "\n"
    )

    with table_path.open("a") as f:
        f.write(output)
