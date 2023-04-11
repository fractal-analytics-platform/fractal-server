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
import logging
import math
from typing import Optional


class SlurmHeuristicsError(ValueError):
    pass


def _estimate_n_parallel_ftasks_per_script(
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
    n_ftasks_tot: int,
    # Optional WorkflowTask attributes:
    n_ftasks_per_script: Optional[int] = None,
    n_parallel_ftasks_per_script: Optional[int] = None,
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
    `n_parallel_ftasks_per_script<n_ftasks_per_script`, that is, where not all
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
        n_ftasks_tot:
            Total number of elements to be processed (e.g. number of images in
            a OME-NGFF array).
        n_ftasks_per_script:
            If `n_ftasks_per_script` and `n_parallel_ftasks_per_script` are not
            `None`, validate/edit this choice.
        n_parallel_ftasks_per_script:
            If `n_ftasks_per_script` and `n_parallel_ftasks_per_script` are not
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
        Valid values of `n_ftasks_per_script` and
        `n_parallel_ftasks_per_script`.
    """
    # Preliminary checks
    if bool(n_ftasks_per_script) != bool(n_parallel_ftasks_per_script):
        msg = (
            "n_ftasks_per_script and n_parallel_ftasks_per_script must "
            "be both set or both unset"
        )
        logging.error(msg)
        raise SlurmHeuristicsError(msg)
    if cpus_per_task > max_cpus_per_job:
        msg = (
            f"[heuristics] Requested {cpus_per_task=} "
            f"but {max_cpus_per_job=}."
        )
        logging.error(msg)
        raise SlurmHeuristicsError(msg)
    if mem_per_task > max_mem_per_job:
        msg = (
            f"[heuristics] Requested {mem_per_task=} "
            f"but {max_mem_per_job=}."
        )
        logging.error(msg)
        raise SlurmHeuristicsError(msg)

    # Branch 1: validate/update given parameters
    if n_ftasks_per_script and n_parallel_ftasks_per_script:
        # Reduce n_parallel_ftasks_per_script if it exceeds n_ftasks_per_script
        if n_parallel_ftasks_per_script > n_ftasks_per_script:
            logging.warning(
                "[heuristics] Set n_parallel_ftasks_per_script="
                f"n_ftasks_per_script={n_ftasks_per_script}"
            )
            n_parallel_ftasks_per_script = n_ftasks_per_script

        # Check requested cpus_per_job
        cpus_per_job = n_parallel_ftasks_per_script * cpus_per_task
        if cpus_per_job > target_cpus_per_job:
            logging.warning(
                f"[heuristics] Requested {cpus_per_job=} "
                f"but {target_cpus_per_job=}."
            )
        if cpus_per_job > max_cpus_per_job:
            msg = (
                f"[heuristics] Requested {cpus_per_job=} "
                f"but {max_cpus_per_job=}."
            )
            logging.error(msg)
            raise SlurmHeuristicsError(msg)

        # Check requested mem_per_job
        mem_per_job = n_parallel_ftasks_per_script * mem_per_task
        if mem_per_job > target_mem_per_job:
            logging.warning(
                f"[heuristics] Requested {mem_per_job=} "
                f"but {target_mem_per_job=}."
            )
        if mem_per_job > max_mem_per_job:
            msg = (
                f"[heuristics] Requested {mem_per_job=} "
                f"but {max_mem_per_job=}."
            )
            logging.error(msg)
            raise SlurmHeuristicsError(msg)

        # Check number of jobs
        num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
        if num_jobs > target_num_jobs:
            logging.info(
                f"[heuristics] Requested {num_jobs=} "
                f"but {target_num_jobs=}."
            )
        if num_jobs > max_num_jobs:
            msg = f"[heuristics] Requested {num_jobs=} but {max_num_jobs=}."
            logging.error(msg)
            raise SlurmHeuristicsError(msg)
        logging.debug("[heuristics] Return from branch 1")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # 2: Target-resources-based heuristics, without in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=target_cpus_per_job,
        max_mem_per_job=target_mem_per_job,
    )
    n_ftasks_per_script = n_parallel_ftasks_per_script
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    if num_jobs <= target_num_jobs:
        logging.debug("[heuristics] Return from branch 2")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # Branch 3: Max-resources-based heuristics, without in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=max_cpus_per_job,
        max_mem_per_job=max_mem_per_job,
    )
    n_ftasks_per_script = n_parallel_ftasks_per_script
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    if num_jobs <= max_num_jobs:
        logging.debug("[heuristics] Return from branch 3")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # Branch 4: Max-resources-based heuristics, with in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        max_cpus_per_job=max_cpus_per_job,
        max_mem_per_job=max_mem_per_job,
    )
    n_ftasks_per_script = math.ceil(n_ftasks_tot / max_num_jobs)
    logging.debug("[heuristics] Return from branch 4")
    return (n_ftasks_per_script, n_parallel_ftasks_per_script)
