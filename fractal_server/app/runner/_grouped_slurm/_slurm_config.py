# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Slurm Bakend

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""
import json
import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from devtools import debug
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

from ....config import get_settings
from ....syringe import Inject
from ...models import WorkflowTask
from .._common import get_task_file_paths
from ..common import TaskParameters


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


class SlurmConfig(BaseModel, extra=Extra.forbid):
    """
    Abstraction for SLURM parameters

    # FIXME: docstring

    # FIXME: check that extra_lines does not overlap with known fields
    """

    # Required SLURM parameters (note that the integer attributes are those
    # that will need to scale up with the number of parallel tasks per job)
    partition: str
    cpus_per_task: int
    mem_per_task_MB: int

    # Optional SLURM parameters
    job_name: Optional[str] = None
    constraint: Optional[str] = None
    gres: Optional[str] = None
    time: Optional[str] = None  # FIXME: this will need to scale up with #tasks
    account: Optional[str] = None

    # Free-field attribute for extra lines to be added to the SLURM job
    # preamble
    extra_lines: Optional[List[str]] = Field(default_factory=list)

    # Metaparameters needed to combine multiple tasks in each SLURM job
    n_ftasks_per_script: Optional[int] = None
    n_parallel_ftasks_per_script: Optional[int] = None
    target_cpus_per_job: int
    max_cpus_per_job: int
    target_mem_per_job: int
    max_mem_per_job: int
    target_num_jobs: int
    max_num_jobs: int

    def preamble_lines(self) -> list[str]:

        raise NotImplementedError()

        # Attributes that need to scale
        scaling_factor = self.n_parallel_ftasks_per_script
        if not scaling_factor:
            raise ValueError()
        debug(scaling_factor)
        mem_per_job_MB = self.mem_per_task_MB * scaling_factor

        lines = []
        lines.append(f"#SBATCH --partition {self.partition}")
        lines.append(f"#SBATCH --cpus-per-task {self.cpus_per_task}")
        lines.append(f"#SBATCH --mem {mem_per_job_MB}M")
        # FIXME: THIS IS NOT COMPLETE...


def get_default_slurm_config():
    """
    FIXME docstring
    """
    return SlurmConfig(
        partition="main",
        cpus_per_task=1,
        mem_per_task_MB=100,
        target_cpus_per_job=1,
        max_cpus_per_job=2,
        target_mem_per_job=100,
        max_mem_per_job=500,
        target_num_jobs=2,
        max_num_jobs=4,
    )


def set_slurm_config(
    wftask: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Path,
    config_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Collect SLURM configuration parameters

    Inject SLURM configuration for single task execution.

    For now, this is the reference implementation for argument
    `submit_setup_call` of
    [fractal_server.app.runner._common.recursive_task_submission][]

    Args:
        task:
            Task for which the sbatch script is to be assembled
        task_pars:
            Task parameters to be passed to the task
            (not used in this function)
        workflow_dir:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_user:
            User-side directory with the same scope as `workflow_dir`, and
            where a user can write.

    Raises:
        SlurmConfigError: if the slurm-configuration file does not contain the
                          required config

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalSlurmExecutor.submit` and `FractalSlurmExecutor.map`, so
            as to set extra options in the sbatch script.
    """

    # Here goes all the logic for reading attributes from the appropriate
    # sources and transforming them into an appropriate SLURM configuration

    # FIXME: replace this hard-coded dict with a file read
    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE
        debug(f"LOADING {settings.FRACTAL_SLURM_CONFIG_FILE=}")
    try:
        with config_path.open("r") as f:  # type: ignore
            slurm_env = json.load(f)
    except Exception as e:
        raise SlurmConfigError(
            f"Error while loading {config_path=}. "
            f"Original error:\n{str(e)}"
        )

    debug("set_slurm_config")
    debug(wftask.overridden_meta)
    debug(slurm_env)
    print("--------------------------------------------")

    slurm_dict = {}

    # Load all relevant attributes from slurm_env
    keys_to_skip = [
        "cpus_per_job",
        "mem_per_job",
        "number_of_jobs",
        "if_needs_gpu",
    ]
    for key, value in slurm_env.items():
        # Skip some keys
        if key in keys_to_skip:
            continue
        # Skip values which are not set (e.g. None or empty strings)
        if not value:
            continue
        # Add this key-value pair to slurm_dict
        slurm_dict[key] = value

    # GPU-related options
    # Notes about priority:
    # 1. This block of definitions takes priority over other definitions from
    #    slurm_env which are not under the `needs_gpu` subgroup
    # 2. This block of definitions has lower priority than whatever comes next
    #    (i.e. from WorkflowTask.overridden_meta).
    needs_gpu = wftask.overridden_meta.get("needs_gpu", False)
    debug(needs_gpu)
    if needs_gpu:
        for key, val in slurm_env["if_needs_gpu"].items():
            debug(key, val)
            # Check that they key is in the list of the valid ones
            if key not in ["partition", "gres", "constraint"]:
                raise ValueError(
                    f"Invalid {key=} in the `if_needs_gpu` section."
                )
            slurm_dict[key] = val

    # Number of CPUs per task, for multithreading
    cpus_per_task = int(wftask.overridden_meta["cpus_per_task"])
    debug(cpus_per_task)
    slurm_dict["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    raw_mem = wftask.overridden_meta["mem"]
    # FIXME: treat "M100M"
    if isinstance(raw_mem, int) or raw_mem.isdigit():
        mem = int(raw_mem)
    elif raw_mem.endswith("M"):
        mem = int(raw_mem.strip("M"))
    elif raw_mem.endswith("G"):
        mem = int(raw_mem.strip("G")) * 10**3
    elif raw_mem.endswith("T"):
        mem = int(raw_mem.strip("T")) * 10**6
    else:
        raise ValueError(
            f"{mem=} is not a valid specification of memory requirements. "
            "Valid examples are: 93, 71M, 93G, 71T."
        )
    slurm_dict["mem_per_task_MB"] = mem

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    slurm_dict["job_name"] = job_name

    # Optional SLURM arguments and extra lines
    for key in ["time", "account", "gres", "constraint"]:
        value = wftask.overridden_meta.get(key, None)
        if value:
            slurm_dict[key] = value
    extra_lines = wftask.overridden_meta.get("extra_lines", [])
    extra_lines = slurm_dict.get("extra_lines", []) + extra_lines
    if len(set(extra_lines)) != len(extra_lines):
        logging.warning(f"Removing repeated elements from {extra_lines=}.")
        extra_lines = list(set(extra_lines))
    slurm_dict["extra_lines"] = extra_lines

    # Job-batching parameters (if None, they will be determined heuristically)
    n_ftasks_per_script = wftask.overridden_meta.get(
        "n_ftasks_per_script", None
    )
    n_parallel_ftasks_per_script = wftask.overridden_meta.get(
        "n_parallel_ftasks_per_script", None
    )
    debug(n_ftasks_per_script)
    debug(n_parallel_ftasks_per_script)

    slurm_dict["n_ftasks_per_script"] = n_ftasks_per_script
    slurm_dict["n_parallel_ftasks_per_script"] = n_parallel_ftasks_per_script
    slurm_dict["target_cpus_per_job"] = slurm_env["cpus_per_job"]["target"]
    slurm_dict["target_mem_per_job"] = slurm_env["mem_per_job"]["target"]
    slurm_dict["target_num_jobs"] = slurm_env["number_of_jobs"]["target"]
    slurm_dict["max_cpus_per_job"] = slurm_env["cpus_per_job"]["max"]
    slurm_dict["max_mem_per_job"] = slurm_env["mem_per_job"]["max"]
    slurm_dict["max_num_jobs"] = slurm_env["number_of_jobs"]["max"]

    # Put everything together
    slurm_config = SlurmConfig(**slurm_dict)

    # Gather information on task files, to be used in wftask_file_prefix and
    # wftask_order
    task_files = get_task_file_paths(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=wftask.order,
    )

    # Prepare and return output dictionary
    submit_setup_dict = dict(
        slurm_config=slurm_config,
        wftask_file_prefix=task_files.file_prefix,
        wftask_order=wftask.order,
    )
    return submit_setup_dict

    """
    config_dict = load_slurm_env()
    try:
        config = config_dict[wftask.executor]
    except KeyError:
        raise SlurmConfigError(f"Configuration not found: {wftask.executor}")

    additional_setup_lines = config.to_sbatch()
    additional_setup_lines.append(
        f"#SBATCH --job-name {wftask.task.name.replace(' ', '_')}"
    )

    # From https://slurm.schedmd.com/sbatch.html: Beginning with 22.05, srun
    # will not inherit the --cpus-per-task value requested by salloc or sbatch.
    # It must be requested again with the call to srun or set with the
    # SRUN_CPUS_PER_TASK environment variable if desired for the task(s).
    if config.cpus_per_task:
        additional_setup_lines.append(
            f"export SRUN_CPUS_PER_TASK={config.cpus_per_task}"
        )
    """
