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
Submodule to handle the SLURM configuration for a WorkflowTask
"""
import json
import logging
from pathlib import Path
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

from ....config import get_settings
from ....syringe import Inject
from ...models import WorkflowTask


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


class SlurmConfig(BaseModel, extra=Extra.forbid):
    """
    Abstraction for SLURM parameters

    Part of the attributes map directly to some of the SLURM attribues (see
    https://slurm.schedmd.com/sbatch.html), e.g. `partition`. Other attributes
    are metaparameters which are needed in fractal-server to combine multiple
    tasks in the same SLURM job (e.g. `n_parallel_ftasks_per_script` or
    `max_num_jobs`).

    # FIXME: check that extra_lines does not overlap with known fields

    Attributes:
        partition: Corresponds to SLURM option.
        cpus_per_task: Corresponds to SLURM option.
        mem_per_task_MB: Corresponds to `mem` SLURM option.
        job_name: Corresponds to `name` SLURM option.
        constraint: Corresponds to SLURM option.
        gres: Corresponds to SLURM option.
        account: Corresponds to SLURM option.
        time: Corresponds to SLURM option (WARNING: not fully supported).

        prefix: Prefix of configuration lines in SLURM submission scripts.
        shebang_line: Shebang line for SLURM submission scripts.
        extra_lines: TBD

        n_ftasks_per_script: TBD
        n_parallel_ftasks_per_script: TBD
        target_cpus_per_job: TBD
        max_cpus_per_job: TBD
        target_mem_per_job: TBD  # FIXME: units?
        max_mem_per_job: TBD
        target_num_jobs: TBD
        max_num_jobs: TBD

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
    extra_lines: Optional[list[str]] = Field(default_factory=list)

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

        """
        FIXME export SRUN_CPUS_PER_TASK
        # From https://slurm.schedmd.com/sbatch.html: Beginning with 22.05,
        # srun will not inherit the --cpus-per-task value requested by salloc
        # or sbatch.  It must be requested again with the call to srun or set
        # with the SRUN_CPUS_PER_TASK environment variable if desired for the
        # task(s).
        if config.cpus_per_task:
            #additional_setup_lines.append(
                f"export SRUN_CPUS_PER_TASK={config.cpus_per_task}"
            )
        """

        return lines


def _parse_mem_value(raw_mem: Union[str, int]) -> int:
    """
    Convert a memory-specification string into an integer (in MB units), or
    simply return the input if it is already an integer.

    Supported units are `"M", "G", "T"`, with `"M"` being the default; some
    parsing examples are: `"10M" -> 10000`, `"3G" -> 3000000`.

    Arguments:
        raw_mem:
            A string (e.g. `"100M"`) or an integer (in MB).

    Returns:
        Integer value of memory in MB units.

    """

    info = f"[_parse_mem_value] {raw_mem=}"
    error_msg = (
        f"{info}, invalid specification of memory requirements "
        "(valid examples: 93, 71M, 93G, 71T)."
    )
    logging.debug(info)

    # Handle integer argument
    if isinstance(raw_mem, int):
        logging.debug(f"{info}, received integer.")
        return raw_mem

    # Handle string argument
    if not raw_mem[0].isdigit():  # fail e.g. for raw_mem="M100"
        logging.error(error_msg)
        raise SlurmConfigError(error_msg)
    if raw_mem.isdigit():
        logging.debug(f"{info}, received digits-only string.")
        mem_MB = int(raw_mem)
    elif raw_mem.endswith("M"):
        logging.debug(f"{info}, received string for memory in M.")
        stripped_raw_mem = raw_mem.strip("M")
        if not stripped_raw_mem.isdigit():
            logging.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem)
    elif raw_mem.endswith("G"):
        logging.debug(f"{info}, received string for memory in G.")
        stripped_raw_mem = raw_mem.strip("G")
        if not stripped_raw_mem.isdigit():
            logging.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**3
    elif raw_mem.endswith("T"):
        logging.debug(f"{info}, received string for memory in T.")
        stripped_raw_mem = raw_mem.strip("T")
        if not stripped_raw_mem.isdigit():
            logging.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**6
    else:
        logging.error(error_msg)
        raise SlurmConfigError(error_msg)

    logging.debug(f"{info}, return {mem_MB}")
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


def get_slurm_config(
    wftask: WorkflowTask,
    workflow_dir: Path,
    workflow_dir_user: Path,
    config_path: Optional[Path] = None,
) -> SlurmConfig:
    """
    FIXME

    Arguments:
        wftask:
            WorkflowTask for which the SLURM configuration is is to be
            prepared.
        workflow_dir:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_user:
            User-side directory with the same scope as `workflow_dir`, and
            where a user can write.
        config_path:
            Path of aFractal  SLURM configuration file; if `None`, use
            `FRACTAL_SLURM_CONFIG_FILE` variable from settings.

    Raises:
        SlurmConfigError: if the slurm-configuration file does not contain the
                          required config

    Returns:
        slurm_config:
            The SlurmConfig object
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

    return slurm_config
