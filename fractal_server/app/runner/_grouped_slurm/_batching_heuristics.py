import logging
import math
from typing import Optional


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

    # Branch 1
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
    reasonable_n_parallel_ftasks_per_script = 2  # FIXME: what is this value??
    if (
        math.ceil(n_ftasks_tot / reasonable_n_parallel_ftasks_per_script)
        <= target_num_jobs
    ):
        n_parallel_ftasks_per_script = reasonable_n_parallel_ftasks_per_script
        n_ftasks_per_script = reasonable_n_parallel_ftasks_per_script
    else:
        n_ftasks_per_script = math.ceil(n_ftasks_tot / target_num_jobs)
        n_parallel_based_on_cpus = int(target_cpus_per_job / cpus_per_task)
        n_parallel_based_on_mem = int(target_mem_per_job / mem_per_task)
        n_parallel_ftasks_per_script = min(
            n_parallel_based_on_cpus, n_parallel_based_on_mem
        )
        n_parallel_ftasks_per_script = min(
            n_parallel_ftasks_per_script, n_ftasks_per_script
        )

    return n_ftasks_per_script, n_parallel_ftasks_per_script
