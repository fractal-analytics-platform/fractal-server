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

from pydantic import BaseModel
from pydantic import Field

from ....config import get_settings
from ....syringe import Inject
from ...models import Workflow
from ...models import WorkflowTask
from .._common import get_workflow_file_paths
from .._common import recursive_task_submission
from ..common import async_wrap
from ..common import TaskParameters
from .executor import FractalSlurmExecutor


class SlurmConfig(BaseModel):
    """
    Abstraction for SLURM executor parameters

    This class wraps options for the `sbatch` command. Attribute `xxx` maps to
    the `--xxx` option of `sbatch`.
    Cf. [sbatch documentation](https://slurm.schedmd.com/sbatch.html)

    Note: options containing hyphens ('-') need be aliased to attribute names
        with underscores ('-').
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
) -> Dict[str, SlurmConfig]:
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
            config_key: SlurmConfig(**raw_data[config_key])
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


def set_slurm_config(
    task: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
) -> Dict[str, Any]:
    """
    Collect SLURM configuration parameters

    Inject SLURM configuration for single task execution.

    For now, this is the reference implementation for argument
    `submit_setup_call` of
    [fractal_server.app.runner._common.recursive_task_submission][]

    Args:
        task:
            The task for which the sbatch script is to be assembled
        task_pars:
            The task parameters to be passed to the task
        workflow_dir:
            The directory in which the executor should store input / output /
            errors from task execution, as well as meta files from the
            submission process.

    Raises:
        SlurmConfigError: if the slurm configuration file does not contain the
        tasks requires.

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalSlurmExecutor.submit` and `FractalSlurmExecutor.map`, so
            as to set extra options in the sbatch script.
    """
    config_dict = load_slurm_config()
    try:
        config = config_dict[task.executor]
    except KeyError:
        raise SlurmConfigError(f"Configuration not found: {task.executor}")

    additional_setup_lines = config.to_sbatch()
    additional_setup_lines.append(
        f"#SBATCH --job-name {task.task.name.replace(' ', '_')}"
    )

    # From https://slurm.schedmd.com/sbatch.html: Beginning with 22.05, srun
    # will not inherit the --cpus-per-task value requested by salloc or sbatch.
    # It must be requested again with the call to srun or set with the
    # SRUN_CPUS_PER_TASK environment variable if desired for the task(s).
    if config.cpus_per_task:
        additional_setup_lines.append(
            f"export SRUN_CPUS_PER_TASK={config.cpus_per_task}"
        )

    workflow_files = get_workflow_file_paths(
        workflow_dir=workflow_dir, task_order=task.order
    )
    return dict(
        additional_setup_lines=additional_setup_lines,
        job_file_prefix=workflow_files.file_prefix,
    )


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: List[Path],
    output_path: Path,
    input_metadata: Dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    username: str = None,
    worker_init: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Internal processing routine for the SLURM backend

    This function initialises the a FractalSlurmExecutor, setting logging,
    workflow working dir and user to impersonate. It then schedules the
    workflow tasks and returns the output dataset metadata.

    Cf. [process_workflow][fractal_server.app.runner._process.process_workflow]
    """
    if isinstance(worker_init, str):
        worker_init = worker_init.split("\n")

    with FractalSlurmExecutor(
        debug=True,
        keep_logs=True,
        username=username,
        script_dir=workflow_dir,
        common_script_lines=worker_init,
    ) as executor:
        output_task_pars_fut = recursive_task_submission(
            executor=executor,
            task_list=workflow.task_list,
            task_pars=TaskParameters(
                input_paths=input_paths,
                output_path=output_path,
                metadata=input_metadata,
                logger_name=logger_name,
            ),
            workflow_dir=workflow_dir,
            submit_setup_call=set_slurm_config,
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
    username: str = None,
    worker_init: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process workflow (SLURM backend public interface)

    Cf. [process_workflow][fractal_server.app.runner._process.process_workflow]
    """
    output_dataset_metadata = await async_wrap(_process_workflow)(
        workflow=workflow,
        input_paths=input_paths,
        output_path=output_path,
        input_metadata=input_metadata,
        logger_name=logger_name,
        workflow_dir=workflow_dir,
        username=username,
        worker_init=worker_init,
    )
    return output_dataset_metadata
