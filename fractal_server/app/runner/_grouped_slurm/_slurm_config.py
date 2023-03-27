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
# import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from devtools import debug
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

from ...models import WorkflowTask
from .._common import get_task_file_paths
from ..common import TaskParameters

# from ....config import get_settings
# from ....syringe import Inject


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


class SlurmConfig(BaseModel, extra=Extra.forbid):
    """
    Abstraction for SLURM parameters

    # FIXME: docstring
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


def set_slurm_config(
    wftask: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Path,
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
    """
    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE
    try:
        with config_path.open("r") as f:  # type: ignore
            raw_data = json.load(f)
    """
    slurm_config = {
        "partition": "main",
        "cpus_per_job": {
            "target": 10,
            "max": 10,
        },
        "mem_per_job": {
            "target": 10,
            "max": 10,
        },
        "number_of_jobs": {
            "target": 10,
            "max": 10,
        },
        "if_needs_gpu": {
            # Possible overrides: partition, gres, constraint
            "partition": "gpu",
            "gres": "gpu:1",
            "constraint": "gpuram32gb",
        },
    }
    debug(slurm_config)

    # REQUIRED ATTRIBUTES
    opt_dict = {}

    # Number of CPUs per task, for multithreading
    cpus_per_task = int(wftask.overridden_meta["cpus_per_task"])
    debug(cpus_per_task)
    opt_dict["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    raw_mem = wftask.overridden_meta["mem"]
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
    debug(mem)
    opt_dict["mem_per_task_MB"] = mem

    # Partition name
    partition = slurm_config["partition"]
    debug(partition)
    opt_dict["partition"] = partition

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    debug(job_name)
    opt_dict["job_name"] = job_name

    # GPU-related options
    needs_gpu = wftask.overridden_meta.get("needs_gpu", False)
    debug(needs_gpu)
    if needs_gpu:
        for key, val in slurm_config["if_needs_gpu"].items():
            if key not in ["partition", "gres", "constraint"]:
                raise ValueError(
                    f"Invalid {key=} in the `if_needs_gpu` section."
                )
            opt_dict[key] = val

    # Optional SLURM arguments and extra lines
    for key in ["time", "account", "gres", "constraint"]:
        value = wftask.overridden_meta.get("time", None)
        if value:
            opt_dict[key] = value
    extra_lines = wftask.overridden_meta.get("extra_lines", None)
    debug(extra_lines)

    # Job-batching parameters (if None, they will be determined heuristically)
    n_ftasks_per_script = wftask.overridden_meta.get(
        "n_ftasks_per_script", None
    )
    n_parallel_ftasks_per_script = wftask.overridden_meta.get(
        "n_parallel_ftasks_per_script", None
    )
    debug(n_ftasks_per_script)
    debug(n_parallel_ftasks_per_script)

    opt_dict["n_ftasks_per_script"] = n_ftasks_per_script
    opt_dict["n_parallel_ftasks_per_script"] = n_parallel_ftasks_per_script
    opt_dict["target_cpus_per_job"] = slurm_config["cpus_per_job"]["target"]
    opt_dict["target_mem_per_job"] = slurm_config["mem_per_job"]["target"]
    opt_dict["target_num_jobs"] = slurm_config["number_of_jobs"]["target"]
    opt_dict["max_cpus_per_job"] = slurm_config["cpus_per_job"]["max"]
    opt_dict["max_mem_per_job"] = slurm_config["mem_per_job"]["max"]
    opt_dict["max_num_jobs"] = slurm_config["number_of_jobs"]["max"]

    # Put everything together
    slurm_options = SlurmConfig(**opt_dict)

    # Gather information on task files, to be used in wftask_file_prefix and
    # wftask_order
    task_files = get_task_file_paths(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=wftask.order,
    )

    # Prepare and return output dictionary
    submit_setup_dict = dict(
        slurm_options=slurm_options,
        wftask_file_prefix=task_files.file_prefix,
        wftask_order=wftask.order,
    )
    return submit_setup_dict

    """
    config_dict = load_slurm_config()
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
