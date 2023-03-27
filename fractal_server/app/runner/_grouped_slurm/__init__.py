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
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from devtools import debug
from pydantic import BaseModel
from pydantic import Field

from ....config import get_settings
from ....syringe import Inject
from ...models import Workflow
from ...models import WorkflowTask
from .._common import get_task_file_paths
from .._common import recursive_task_submission
from ..common import async_wrap
from ..common import TaskParameters
from .executor import FractalSlurmExecutor


class OldSlurmConfig(BaseModel):
    """
    Abstraction for SLURM executor parameters

    This class wraps options for the `sbatch` command. Attribute `xxx_yyy` maps
    to the `--xxx-yyy` option of `sbatch`. Cf. [sbatch
    documentation](https://slurm.schedmd.com/sbatch.html)

    Note that options containing hyphens ('-') need be aliased to attribute
    names with underscores ('-').

    Attributes:
        partition: TBD
        time: TBD
        mem:
            TBD
            From sbatch docs: Default units are megabytes. Different units can
            be specified using the suffix [K|M|G|T]
        cpus_per_task: TBD
        account: TBD
        extra_lines: TBD
    """

    class Config:
        allow_population_by_field_name = True

    partition: str
    time: Optional[str]
    mem: Optional[str]
    cpus_per_task: Optional[str] = Field(alias="cpus-per-task")
    account: Optional[str]
    extra_lines: Optional[List[str]] = Field(default_factory=list)

    def to_sbatch(self, prefix="#SBATCH "):
        """
        Transform the class attributes into the preamble block of a script to
        be submitted via `sbatch`
        """
        dic = self.dict(
            exclude_none=True, by_alias=True, exclude={"extra_lines"}
        )
        sbatch_lines = []
        for k, v in dic.items():
            sbatch_lines.append(f"{prefix}--{k}={v}")
        sbatch_lines.extend(self.extra_lines)
        return sbatch_lines


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


def load_slurm_config(
    config_path: Optional[Path] = None,
) -> Dict[str, OldSlurmConfig]:
    """
    Parse slurm configuration file

    The configuration file can contain multiple SLURM configurations in JSON
    format. This functions deserialises all the configurations and returns
    them in the form of SlurmConfig objects.

    Args:
        config_path:
            The path to the configuration file. If not provided, it is read
            from Fractal settings.

    Raises:
        SlurmConfigError: if any exeception was raised in reading or
                          deserialising the configuration file.

    Returns:
        config_dict:
            Dictionary whose keys are the configuration identifiers and whose
            values are SlurmConfig objects.
    """
    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE
    try:
        with config_path.open("r") as f:  # type: ignore
            raw_data = json.load(f)

        # coerce
        config_dict = {
            config_key: OldSlurmConfig(**raw_data[config_key])
            for config_key in raw_data
        }
    except FileNotFoundError:
        raise SlurmConfigError(f"Configuration file not found: {config_path}")
    except Exception as e:
        raise SlurmConfigError(
            f"Could not read slurm configuration file: {config_path}"
            f"\nOriginal error: {repr(e)}"
        )
    return config_dict


class SlurmConfig(BaseModel):
    """
    Abstraction for SLURM parameters
    """

    # Required SLURM parameters (note that the integer attributes are those
    # that will need to scale up with the number of parallel tasks per job)
    partition: str
    cpus_per_task: int
    mem_per_task_MB: int

    # Optional SLURM parameters
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
    wftask_options = {}

    # Number of CPUs per task, for multithreading
    cpus_per_task = int(wftask.overridden_meta["cpus_per_task"])
    debug(cpus_per_task)
    wftask_options["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    raw_mem = wftask.overridden_meta["mem"]
    if raw_mem.isdigit():
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
    wftask_options["mem_per_task_MB"] = mem

    # Partition name
    partition = slurm_config["partition"]
    debug(partition)
    wftask_options["partition"] = partition

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    debug(job_name)
    wftask_options["job_name"] = job_name
    # GPU-related options
    needs_gpu = wftask.overridden_meta["needs_gpu"]
    debug(needs_gpu)
    if needs_gpu:
        for key, val in slurm_config["if_needs_gpu"].items():
            if key not in ["partition", "gres", "constraint"]:
                raise ValueError(
                    f"Invalid {key=} in the `if_needs_gpu` section."
                )
            wftask_options[key] = val

    # OPTIONAL ARGUMENTS

    for key in ["time", "account", "gres", "constraint"]:
        value = wftask.overridden_meta.get("time", None)
        if value:
            wftask_options[key] = value

    # Job-batching parameters (if None, they will be determined heuristically)
    n_ftasks_per_script = wftask.overridden_meta["n_ftasks_per_script"]
    n_parallel_ftasks_per_script = wftask.overridden_meta[
        "n_parallel_ftasks_per_script"
    ]  # noqa
    debug(n_ftasks_per_script)
    debug(n_parallel_ftasks_per_script)

    # Extra lines
    extra_lines = wftask.overridden_meta.get("extra_lines", None)
    debug(extra_lines)

    # Put everything together
    slurm_options = SlurmConfig(**wftask_options)

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


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: List[Path],
    output_path: Path,
    input_metadata: Dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    workflow_dir_user: Path,
    slurm_user: Optional[str] = None,
    worker_init: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Internal processing routine for the SLURM backend

    This function initialises the a FractalSlurmExecutor, setting logging,
    workflow working dir and user to impersonate. It then schedules the
    workflow tasks and returns the output dataset metadata.

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]

    Returns:
        output_dataset_metadata: Metadata of the output dataset
    """

    if not slurm_user:
        raise RuntimeError(
            "slurm_user argument is required, for slurm backend"
        )

    if isinstance(worker_init, str):
        worker_init = worker_init.split("\n")

    with FractalSlurmExecutor(
        debug=True,
        keep_logs=True,
        slurm_user=slurm_user,
        working_dir=workflow_dir,
        working_dir_user=workflow_dir_user,
        common_script_lines=worker_init,
    ) as executor:
        output_task_pars_fut = recursive_task_submission(
            executor=executor,
            task_list=workflow.task_list,
            task_pars=TaskParameters(
                input_paths=input_paths,
                output_path=output_path,
                metadata=input_metadata,
            ),
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
            submit_setup_call=set_slurm_config,
            logger_name=logger_name,
        )
    output_task_pars = output_task_pars_fut.result()
    output_dataset_metadata = output_task_pars.metadata
    return output_dataset_metadata


async def process_workflow(
    *,
    workflow: Workflow,
    input_paths: List[Path],
    output_path: Path,
    input_metadata: Dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    slurm_user: Optional[str] = None,
    worker_init: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process workflow (SLURM backend public interface)

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]
    """
    output_dataset_metadata = await async_wrap(_process_workflow)(
        workflow=workflow,
        input_paths=input_paths,
        output_path=output_path,
        input_metadata=input_metadata,
        logger_name=logger_name,
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        slurm_user=slurm_user,
        worker_init=worker_init,
    )
    return output_dataset_metadata
