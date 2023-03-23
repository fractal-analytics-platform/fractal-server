import logging
import math

import pytest
from devtools import debug


def heuristics(
    *,
    # Number of parallel componens (always known)
    n_ftasks_tot: int,
    # Optional WorkflowTask attributes:
    n_ftasks_per_script: int | None,
    n_parallel_ftasks_per_script: int | None,
    # Task requirements (multiple possible sources):
    cpus_per_task: int,
    mem_per_task: int,
    # Fractal configuration variables (soft/hard limits):
    target_cpus_per_job: int,
    max_cpus_per_job: int,
    target_mem_per_job: int,
    max_mem_per_job: int,
    target_num_jobs: int,
    max_num_jobs: int,
) -> tuple[int, int]:

    # TODO: memory may come as a string, with units. Here we should parse it,
    # and convert it to a given unit (say MB)
    # (maybe this will happen somewhere else, and here we only deal with K..)

    # Branch 1
    if bool(n_ftasks_per_script) != bool(n_parallel_ftasks_per_script):
        raise ValueError(
            "n_ftasks_per_script and n_parallel_ftasks_per_script must "
            "be both set or both unset"
        )
    if n_ftasks_per_script and n_parallel_ftasks_per_script:
        if n_parallel_ftasks_per_script > n_ftasks_per_script:
            logging.warning(
                "Setting n_parallel_ftasks_per_script="
                f"n_ftasks_per_script={n_ftasks_per_script}"
            )
            n_parallel_ftasks_per_script = n_ftasks_per_script
        if n_parallel_ftasks_per_script * cpus_per_task > target_cpus_per_job:
            logging.warning("Requesting more cpus than expected")
        if n_parallel_ftasks_per_script * cpus_per_task > max_cpus_per_job:
            raise ValueError("Requesting more cpus than allowed")
        if n_parallel_ftasks_per_script * mem_per_task > target_mem_per_job:
            logging.warning("Requesting more memory than expected")
        if n_parallel_ftasks_per_script * mem_per_task > max_mem_per_job:
            raise ValueError("Requesting more memory than allowed")
        num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
        if num_jobs > target_num_jobs:
            logging.warning("Requesting more jobs than expected")
        if num_jobs > max_num_jobs:
            raise ValueError("Requesting more jobs than allowed")
        return n_ftasks_per_script, n_parallel_ftasks_per_script

    # Branch 2
    reasonable_n_parallel_ftasks_per_script = 1
    if (
        math.ceil(n_ftasks_tot / reasonable_n_parallel_ftasks_per_script)
        <= target_num_jobs
    ):  # noqa
        n_parallel_ftasks_per_script = reasonable_n_parallel_ftasks_per_script
        n_ftasks_per_script = reasonable_n_parallel_ftasks_per_script
    else:
        n_ftasks_per_script = math.ceil(n_ftasks_tot / target_num_jobs)
        n_parallel_based_on_cpus = target_cpus_per_job / cpus_per_task
        n_parallel_based_on_mem = target_mem_per_job / mem_per_task
        n_parallel_ftasks_per_script = min(
            n_parallel_based_on_cpus, n_parallel_based_on_mem
        )  # noqa
        n_parallel_ftasks_per_script = min(
            n_parallel_ftasks_per_script, n_ftasks_per_script
        )  # noqa

    return n_ftasks_per_script, n_parallel_ftasks_per_script


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
