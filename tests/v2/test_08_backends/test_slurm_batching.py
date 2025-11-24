from copy import copy

import pytest

from fractal_server.runner.executors.slurm_common._batching import (
    SlurmHeuristicsError,
)
from fractal_server.runner.executors.slurm_common._batching import heuristics


def test_heuristics():
    args = dict(
        tot_tasks=10,
        cpus_per_task=2,
        mem_per_task=1_000,
        target_cpus_per_job=4,
        max_cpus_per_job=16,
        target_mem_per_job=10_000,
        max_mem_per_job=32_000,
        target_num_jobs=100,
        max_num_jobs=1_000,
    )

    with pytest.raises(SlurmHeuristicsError):
        heuristics(
            tasks_per_job=1,
            parallel_tasks_per_job=None,
            **args,
        )

    current_args = copy(args) | dict(cpus_per_task=100)
    with pytest.raises(SlurmHeuristicsError):
        heuristics(**current_args)

    current_args = copy(args) | dict(mem_per_task=100_000)
    with pytest.raises(SlurmHeuristicsError):
        heuristics(**current_args)

    current_args = copy(args) | dict(tasks_per_job=2, parallel_tasks_per_job=5)
    new_tasks_per_job, new_parallel_tasks_per_job = heuristics(**current_args)
    assert new_tasks_per_job == 2
    assert new_parallel_tasks_per_job == 2

    current_args = copy(args) | dict(
        cpus_per_task=10, tasks_per_job=2, parallel_tasks_per_job=2
    )
    with pytest.raises(SlurmHeuristicsError):
        heuristics(**current_args)

    current_args = copy(args) | dict(
        mem_per_task=20_000, tasks_per_job=2, parallel_tasks_per_job=2
    )
    with pytest.raises(SlurmHeuristicsError):
        heuristics(**current_args)

    current_args = copy(args) | dict(
        tot_tasks=100_000, tasks_per_job=2, parallel_tasks_per_job=2
    )
    with pytest.raises(SlurmHeuristicsError):
        heuristics(**current_args)

    NUM_TASKS = 10000
    tasks_per_job, parallel_tasks_per_job = heuristics(
        tot_tasks=NUM_TASKS,
        cpus_per_task=1,
        mem_per_task=100,
        target_cpus_per_job=1,
        max_cpus_per_job=1,
        target_mem_per_job=100,
        max_mem_per_job=100,
        max_num_jobs=1,
        target_num_jobs=10,
    )
    assert tasks_per_job == NUM_TASKS
    assert parallel_tasks_per_job == 1
