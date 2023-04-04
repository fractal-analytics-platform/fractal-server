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
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

from ....config import get_settings
from ....syringe import Inject
from ...models import WorkflowTask
from .._common import get_task_file_paths
from .._common import TaskFiles
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
    prefix: str = "#SBATCH"
    shebang_line: str = "#!/bin/sh"

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

    def _sorted_extra_lines(self) -> list[str]:
        """
        Return a copy of self.extra_lines, where lines starting with
        `self.prefix` are listed first.
        """

        def _no_prefix(_line):
            if _line.startswith(self.prefix):
                return 0
            else:
                return 1

        return sorted(self.extra_lines, key=_no_prefix)

    def sort_script_lines(self, script_lines: list[str]) -> list[str]:
        """
        Return a copy of `script_lines`, where lines are sorted as in:
        1. `self.shebang_line` (if present);
        2. Lines starting with `self.prefix`;
        3. Other lines.
        """

        def _sorting_function(_line):
            if _line == self.shebang_line:
                return 0
            elif _line.startswith(self.prefix):
                return 1
            else:
                return 2

        return sorted(script_lines, key=_sorting_function)

    def to_sbatch_preamble(self) -> list[str]:
        """
        FIXME: docstring of to_sbatch_preamble
        """
        if self.n_parallel_ftasks_per_script is None:
            raise ValueError(
                "SlurmConfig.sbatch_preamble requires that "
                f"{self.n_parallel_ftasks_per_script=} is not None."
            )
        if self.extra_lines:
            if len(self.extra_lines) != len(set(self.extra_lines)):
                raise ValueError(f"{self.extra_lines=} contains repetitions")

        mem_per_job_MB = (
            self.n_parallel_ftasks_per_script * self.mem_per_task_MB
        )
        lines = [
            self.shebang_line,
            f"{self.prefix} --partition={self.partition}",
            f"{self.prefix} --ntasks={self.n_parallel_ftasks_per_script}",
            f"{self.prefix} --cpus-per-task={self.cpus_per_task}",
            f"{self.prefix} --mem={mem_per_job_MB}M",
        ]
        for key in ["job_name", "constraint", "gres", "time", "account"]:
            value = getattr(self, key)
            if value is not None:
                option = key.replace("_", "-")
                lines.append(f"{self.prefix} --{option}={value}")
        if self.extra_lines:
            for line in self._sorted_extra_lines():
                lines.append(line)

        return lines


def _parse_mem_value(raw_mem):
    """
    FIXME: add docstring and unit test
    """
    error_msg = (
        f'"{raw_mem}" is not a valid specification of memory '
        "requirements. Some valid examples: 93, 71M, 93G, 71T."
    )

    logging.warning(f"[_parse_mem_value] {raw_mem=}")

    if isinstance(raw_mem, int):
        logging.warning("[_parse_mem_value] 0a")
        return raw_mem

    # Preliminary check
    if not raw_mem[0].isdigit():
        logging.warning("[_parse_mem_value] 0b")
        raise ValueError(error_msg)

    if raw_mem.isdigit():
        mem_MB = int(raw_mem)
        logging.warning("[_parse_mem_value] 1")
    elif raw_mem.endswith("M"):
        mem_MB = int(raw_mem.strip("M"))
        logging.warning("[_parse_mem_value] 2")
    elif raw_mem.endswith("G"):
        mem_MB = int(raw_mem.strip("G")) * 10**3
        logging.warning("[_parse_mem_value] 3")
    elif raw_mem.endswith("T"):
        mem_MB = int(raw_mem.strip("T")) * 10**6
        logging.warning("[_parse_mem_value] 4")
    else:
        raise ValueError(error_msg)

    logging.warning(f"[_parse_mem_value] {mem_MB=}")

    return mem_MB


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
) -> Dict[str, Union[TaskFiles, SlurmConfig]]:
    """
    Collect WorfklowTask-specific configuration parameters from different
    sources, and inject them for execution

    Here goes all the logic for reading attributes from the appropriate sources
    and transforming them into an appropriate SLURM configuration

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

    # Read Fracatal SLURM configuration file
    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE
    logging.warning(f"Now loading {config_path=}")
    try:
        with config_path.open("r") as f:  # type: ignore
            slurm_env = json.load(f)
    except Exception as e:
        raise SlurmConfigError(
            f"Error while loading {config_path=}. "
            f"Original error:\n{str(e)}"
        )
    logging.warning(
        f"WorkflowTask/Task meta attribute: {wftask.overridden_meta=}"
    )

    slurm_dict = {}

    # Load all relevant attributes from slurm_env
    keys_to_skip = [
        "fractal_task_batching",
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
        if key == "mem":
            mem_per_task_MB = _parse_mem_value(value)
            slurm_dict["mem_per_task_MB"] = mem_per_task_MB
        else:
            slurm_dict[key] = value
    batching_dict = slurm_env["fractal_task_batching"]
    slurm_dict["target_cpus_per_job"] = batching_dict["target_cpus_per_job"]
    slurm_dict["max_cpus_per_job"] = batching_dict["max_cpus_per_job"]
    slurm_dict["target_mem_per_job"] = _parse_mem_value(
        batching_dict["target_mem_per_job"]
    )
    slurm_dict["max_mem_per_job"] = _parse_mem_value(
        batching_dict["max_mem_per_job"]
    )
    slurm_dict["target_num_jobs"] = batching_dict["target_num_jobs"]
    slurm_dict["max_num_jobs"] = batching_dict["max_num_jobs"]

    logging.warning(f"Fractal SLURM configuration file: {slurm_env=}")
    logging.warning(f"Options retained: {slurm_dict=}")

    # GPU-related options
    # Notes about priority:
    # 1. This block of definitions takes priority over other definitions from
    #    slurm_env which are not under the `needs_gpu` subgroup
    # 2. This block of definitions has lower priority than whatever comes next
    #    (i.e. from WorkflowTask.overridden_meta).
    needs_gpu = wftask.overridden_meta.get("needs_gpu", False)
    logging.warning(f"{needs_gpu=}")
    if needs_gpu:
        for key, value in slurm_env["if_needs_gpu"].items():
            logging.warning(f"if_needs_gpu options: {key=}, {value=}")
            if key == "mem":
                mem_per_task_MB = _parse_mem_value(value)
                slurm_dict["mem_per_task_MB"] = mem_per_task_MB
            else:
                slurm_dict[key] = value
    logging.warning(f"After {needs_gpu=}, {slurm_dict=}")

    # Number of CPUs per task, for multithreading
    if "cpus_per_task" in wftask.overridden_meta.keys():
        cpus_per_task = int(wftask.overridden_meta["cpus_per_task"])
        logging.warning(cpus_per_task)
        slurm_dict["cpus_per_task"] = cpus_per_task
    logging.warning(f"After cpus_per_task block, {slurm_dict=}")

    # Required memory per task, in MB
    if "mem" in wftask.overridden_meta.keys():
        raw_mem = wftask.overridden_meta["mem"]
        mem_per_task_MB = _parse_mem_value(raw_mem)
        slurm_dict["mem_per_task_MB"] = mem_per_task_MB
    logging.warning(f"After mem block, {slurm_dict=}")

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    slurm_dict["job_name"] = job_name
    logging.warning(f"After job_name block, {slurm_dict=}")

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
    logging.warning(f"After extra_lines block, {slurm_dict=}")

    # Job-batching parameters (if None, they will be determined heuristically)
    n_ftasks_per_script = wftask.overridden_meta.get(
        "n_ftasks_per_script", None
    )
    n_parallel_ftasks_per_script = wftask.overridden_meta.get(
        "n_parallel_ftasks_per_script", None
    )
    logging.warning(f"{n_ftasks_per_script=}")
    logging.warning(f"{n_parallel_ftasks_per_script=}")

    slurm_dict["n_ftasks_per_script"] = n_ftasks_per_script
    slurm_dict["n_parallel_ftasks_per_script"] = n_parallel_ftasks_per_script

    # Put everything together
    logging.warning(f"Now create a SlurmConfig object based on {slurm_dict=}")
    slurm_config = SlurmConfig(**slurm_dict)
    logging.warning(f"{slurm_config=}")

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
        task_files=task_files,
        # wftask_file_prefix=task_files.file_prefix,  # FIXME remove
        # wftask_order=wftask.order,  # FIXME remove
    )
    return submit_setup_dict

    """
    # From https://slurm.schedmd.com/sbatch.html: Beginning with 22.05, srun
    # will not inherit the --cpus-per-task value requested by salloc or sbatch.
    # It must be requested again with the call to srun or set with the
    # SRUN_CPUS_PER_TASK environment variable if desired for the task(s).
    if config.cpus_per_task:
        #additional_setup_lines.append(
            f"export SRUN_CPUS_PER_TASK={config.cpus_per_task}"
        )
    """
