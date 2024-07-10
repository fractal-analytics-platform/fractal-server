# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Submodule to determine the number of total/parallel tasks per SLURM job.
"""
import math
from typing import Optional

from .....logger import set_logger

logger = set_logger(__name__)


class SlurmHeuristicsError(ValueError):
    pass


def _estimate_parallel_tasks_per_job(
    *,
    cpus_per_task: int,
    mem_per_task: int,
    max_cpus_per_job: int,
    max_mem_per_job: int,
) -> int:
    """
    Compute how many parallel tasks can fit in a given SLURM job

    Note: If more resources than available are requested, return 1. This
    assumes that further checks will be performed on the output of the current
    function, as is the case in the `heuristics` function below.

    Arguments:
        cpus_per_task: Number of CPUs needed for one task.
        mem_per_task: Memory (in MB) needed for one task.
        max_cpus_per_job: Maximum number of CPUs available for one job.
        max_mem_per_job: Maximum memory (in MB) available for one job.

    Returns:
        Number of parallel tasks per job
    """
    if cpus_per_task > max_cpus_per_job or mem_per_task > max_mem_per_job:
        return 1
    val_based_on_cpus = max_cpus_per_job // cpus_per_task
    val_based_on_mem = max_mem_per_job // mem_per_task
    return min(val_based_on_cpus, val_based_on_mem)


def heuristics(
    *,
    # Number of parallel components (always known)
    tot_tasks: int,
    # Optional WorkflowTask attributes:
    tasks_per_job: Optional[int] = None,
    parallel_tasks_per_job: Optional[int] = None,
    # Task requirements (multiple possible sources):
    cpus_per_task: int,
    mem_per_task: int,
    # Fractal configuration variables (soft/hard limits):
    target_cpus_per_job: int,
    max_cpus_per_job: int,
    target_mem_per_job: int,  # in MB
    max_mem_per_job: int,  # in MB
    target_num_jobs: int,
    max_num_jobs: int,
) -> tuple[int, int]:
    """
    Heuristically determine parameters for multi-task batching

    "In-job queues" refer to the case where
    `parallel_tasks_per_job<tasks_per_job`, that is, where not all
    tasks of a given SLURM job will be executed at the same time.

    This function goes through the following branches:

    1. Validate/fix parameters, if they are provided as input.
    2. Heuristically determine parameters based on the per-task resource
       requirements and on the target amount of per-job resources, without
       resorting to in-job queues.
    3. Heuristically determine parameters based on the per-task resource
       requirements and on the maximum amount of per-job resources, without
       resorting to in-job queues.
    4. Heuristically determine parameters (based on the per-task resource
       requirements and on the maximum amount of per-job resources) and then
       introduce in-job queues to satisfy the hard constraint on the maximum
       number of jobs.

    Arguments:
        tot_tasks:
            Total number of elements to be processed (e.g. number of images in
            a OME-NGFF array).
        tasks_per_job:
            If `tasks_per_job` and `parallel_tasks_per_job` are not
            `None`, validate/edit this choice.
        parallel_tasks_per_job:
            If `tasks_per_job` and `parallel_tasks_per_job` are not
            `None`, validate/edit this choice.
        cpus_per_task:
            Number of CPUs needed for each parallel task.
        mem_per_task:
            Memory (in MB) needed for each parallel task.
        target_cpus_per_job:
            Optimal number of CPUs for each SLURM job.
        max_cpus_per_job:
            Maximum number of CPUs for each SLURM job.
        target_mem_per_job:
            Optimal amount of memory (in MB) for each SLURM job.
        max_mem_per_job:
            Maximum amount of memory (in MB) for each SLURM job.
        target_num_jobs:
            Optimal total number of SLURM jobs for a given WorkflowTask.
        max_num_jobs:
            Maximum total number of SLURM jobs for a given WorkflowTask.
    Return:
        Valid values of `tasks_per_job` and `parallel_tasks_per_job`.
    """
    # Preliminary checks
    if bool(tasks_per_job) != bool(parallel_tasks_per_job):
        msg = (
            "tasks_per_job and parallel_tasks_per_job must "
            "be both set or both unset"
        )
        logger.error(msg)
        raise SlurmHeuristicsError(msg)
    if cpus_per_task > max_cpus_per_job:
        msg = (
            f"[heuristics] Requested {cpus_per_task=} "
            f"but {max_cpus_per_job=}."
        )
        logger.error(msg)
        raise SlurmHeuristicsError(msg)
    if mem_per_task > max_mem_per_job:
        msg = (
            f"[heuristics] Requested {mem_per_task=} "
            f"but {max_mem_per_job=}."
        )
        logger.error(msg)
        raise SlurmHeuristicsError(msg)

    # Branch 1: validate/update given parameters
    if tasks_per_job and parallel_tasks_per_job:
        # Reduce parallel_tasks_per_job if it exceeds tasks_per_job
        if parallel_tasks_per_job > tasks_per_job:
            logger.warning(
                "[heuristics] Set parallel_tasks_per_job="
                f"tasks_per_job={tasks_per_job}"
            )
            parallel_tasks_per_job = tasks_per_job

        # Check requested cpus_per_job
        cpus_per_job = parallel_tasks_per_job * cpus_per_task
        if cpus_per_job > target_cpus_per_job:
            logger.warning(
                f"[heuristics] Requested {cpus_per_job=} "
                f"but {target_cpus_per_job=}."
            )
        if cpus_per_job > max_cpus_per_job:
            msg = (
                f"[heuristics] Requested {cpus_per_job=} "
                f"but {max_cpus_per_job=}."
            )
            logger.error(msg)
            raise SlurmHeuristicsError(msg)

        # Check requested mem_per_job
        mem_per_job = parallel_tasks_per_job * mem_per_task
        if mem_per_job > target_mem_per_job:
            logger.warning(
                f"[heuristics] Requested {mem_per_job=} "
                f"but {target_mem_per_job=}."
            )
        if mem_per_job > max_mem_per_job:
            msg = (
                f"[heuristics] Requested {mem_per_job=} "
                f"but {max_mem_per_job=}."
            )
            logger.error(msg)
            raise SlurmHeuristicsError(msg)

        # Check number of jobs
        num_jobs = math.ceil(tot_tasks / tasks_per_job)
        if num_jobs > target_num_jobs:
            logger.debug(
                f"[heuristics] Requested {num_jobs=} "
                f"but {target_num_jobs=}."
            )
        if num_jobs > max_num_jobs:
            msg = f"[heuristics] Requested {num_jobs=} but {max_num_jobs=}."
            logger.error(msg)
            raise SlurmHeuristicsError(msg)
        logger.debug("[heuristics] Return from branch 1")
        return (tasks_per_job, parallel_tasks_per_job)

    # 2: Target-resources-based heuristics, without in-job queues
    parallel_tasks_per_job = _estimate_parallel_tasks_per_job(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=target_cpus_per_job,
        max_mem_per_job=target_mem_per_job,
    )
    tasks_per_job = parallel_tasks_per_job
    num_jobs = math.ceil(tot_tasks / tasks_per_job)
    if num_jobs <= target_num_jobs:
        logger.debug("[heuristics] Return from branch 2")
        return (tasks_per_job, parallel_tasks_per_job)

    # Branch 3: Max-resources-based heuristics, without in-job queues
    parallel_tasks_per_job = _estimate_parallel_tasks_per_job(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=max_cpus_per_job,
        max_mem_per_job=max_mem_per_job,
    )
    tasks_per_job = parallel_tasks_per_job
    num_jobs = math.ceil(tot_tasks / tasks_per_job)
    if num_jobs <= max_num_jobs:
        logger.debug("[heuristics] Return from branch 3")
        return (tasks_per_job, parallel_tasks_per_job)

    # Branch 4: Max-resources-based heuristics, with in-job queues
    parallel_tasks_per_job = _estimate_parallel_tasks_per_job(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=max_cpus_per_job,
        max_mem_per_job=max_mem_per_job,
    )
    tasks_per_job = math.ceil(tot_tasks / max_num_jobs)
    logger.debug("[heuristics] Return from branch 4")
    return (tasks_per_job, parallel_tasks_per_job)
