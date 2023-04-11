import logging
import math
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner._slurm._batching import heuristics
from fractal_server.app.runner._slurm._batching import SlurmHeuristicsError


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
    """
    This test produces an example table of heuristic results, and also checks
    that n_parallel_ftasks_per_script is never set to 0 in these cases.
    """

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
        raise ValueError(f"{n_parallel_ftasks_per_script=}")

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


def test_validate_existing_choice(caplog):
    """
    Test different scenarios of calling heuristics with non-None values of
    n_ftasks_per_script and n_parallel_ftasks_per_script.
    """

    caplog.set_level(logging.WARNING)

    base_kwargs = dict(
        n_ftasks_tot=20,  # number of wells
        cpus_per_task=1,
        target_cpus_per_job=4,
        max_cpus_per_job=12,
        mem_per_task=1000,
        target_mem_per_job=8000,
        max_mem_per_job=16000,
        target_num_jobs=5,
        max_num_jobs=10,
    )

    # FAIL for too many jobs
    kw = base_kwargs.copy()
    kw["n_ftasks_per_script"] = 1
    kw["n_parallel_ftasks_per_script"] = 1
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "Requested num_jobs" in e.value.args[0]

    # FAIL for too many CPUs requested
    kw = base_kwargs.copy()
    kw["n_ftasks_per_script"] = 20
    kw["n_parallel_ftasks_per_script"] = 20
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "Requested cpus_per_job" in e.value.args[0]

    # FAIL for too much memory requested
    kw = base_kwargs.copy()
    kw["max_cpus_per_job"] = 1000
    kw["n_ftasks_per_script"] = 20
    kw["n_parallel_ftasks_per_script"] = 20
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "Requested mem_per_job" in e.value.args[0]

    # WARNING for edit of n_parallel_ftasks_per_script=
    caplog.clear()
    kw["n_ftasks_per_script"] = 4
    kw["n_parallel_ftasks_per_script"] = 5
    n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(caplog.text)
    warning_msg = "Set n_parallel_ftasks_per_script=n_ftasks_per_script"
    assert warning_msg in caplog.text

    # All good
    caplog.clear()
    kw["n_ftasks_per_script"] = 4
    kw["n_parallel_ftasks_per_script"] = 4
    n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(caplog.text)


def test_failures():
    """
    Test different failur scenarios
    """

    base_kwargs = dict(
        n_ftasks_tot=20,  # number of wells
        cpus_per_task=1,
        target_cpus_per_job=4,
        max_cpus_per_job=12,
        mem_per_task=1000,
        target_mem_per_job=8000,
        max_mem_per_job=16000,
        target_num_jobs=5,
        max_num_jobs=10,
    )

    # FAIL for only setting one of n_ftasks_per_script
    # n_parallel_ftasks_per_script
    kw = base_kwargs.copy()
    kw["n_ftasks_per_script"] = 1
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "must be both set or both unset" in e.value.args[0]
    kw = base_kwargs.copy()
    kw["n_parallel_ftasks_per_script"] = 1
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "must be both set or both unset" in e.value.args[0]

    # FAIL for asking more cpus_per_task than allowed
    kw = base_kwargs.copy()
    kw["cpus_per_task"] = 100
    kw["max_cpus_per_job"] = 10
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "[heuristics] Requested cpus_per_task=" in e.value.args[0]
    assert "but max_cpus_per_job=" in e.value.args[0]

    # FAIL for asking more memory than allowed
    kw = base_kwargs.copy()
    kw["mem_per_task"] = 128000
    kw["max_mem_per_job"] = 64000
    with pytest.raises(SlurmHeuristicsError) as e:
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(**kw)
    debug(e.value.args[0])
    assert "[heuristics] Requested mem_per_task=" in e.value.args[0]
    assert "but max_mem_per_job=" in e.value.args[0]
