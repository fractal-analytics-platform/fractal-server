import logging
import math
from typing import Optional


def _estimate_n_parallel_ftasks_per_script(
    *,
    cpus_per_task: int,
    mem_per_task: int,
    ref_cpus_per_job: int,
    ref_mem_per_job: int,
) -> int:
    if cpus_per_task > ref_cpus_per_job or mem_per_task > ref_mem_per_job:
        return 1
    val_based_on_cpus = ref_cpus_per_job // cpus_per_task
    val_based_on_mem = ref_mem_per_job // mem_per_task
    return min(val_based_on_cpus, val_based_on_mem)


def heuristics(
    *,
    # Number of parallel componens (always known)
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
    Heuristics to validate/determine n_ftasks_per_script and
    n_parallel_ftasks_per_script

    FIXME docstring
    """
    # Preliminary check
    if bool(n_ftasks_per_script) != bool(n_parallel_ftasks_per_script):
        raise ValueError(
            "n_ftasks_per_script and n_parallel_ftasks_per_script must "
            "be both set or both unset"
        )

    # Branch 1: validate/update given parameters
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
        logging.critical("Heuristic: branch 1")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # Branch 2a: Target-based heuristics, without in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        ref_cpus_per_job=target_cpus_per_job,
        ref_mem_per_job=target_mem_per_job,
    )
    n_ftasks_per_script = n_parallel_ftasks_per_script  # no in-job queues
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    if num_jobs <= target_num_jobs:
        logging.critical("Heuristic: branch 2a")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # Branch 2b: Max-based heuristics, without in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        ref_cpus_per_job=max_cpus_per_job,
        ref_mem_per_job=max_mem_per_job,
    )
    n_ftasks_per_script = n_parallel_ftasks_per_script  # no in-job queues
    num_jobs = math.ceil(n_ftasks_tot / n_ftasks_per_script)
    if num_jobs <= max_num_jobs:
        logging.critical("Heuristic: branch 2b")
        return (n_ftasks_per_script, n_parallel_ftasks_per_script)

    # Branch 3: Max-based heuristics, with in-job queues
    n_parallel_ftasks_per_script = _estimate_n_parallel_ftasks_per_script(
        cpus_per_task=cpus_per_task,
        mem_per_task=mem_per_task,
        ref_cpus_per_job=max_cpus_per_job,
        ref_mem_per_job=max_mem_per_job,
    )
    n_ftasks_per_script = math.ceil(n_ftasks_tot / max_num_jobs)
    logging.critical("Heuristic: branch 3")
    return (n_ftasks_per_script, n_parallel_ftasks_per_script)
