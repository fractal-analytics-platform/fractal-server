import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel

from ....config import get_settings
from ....syringe import Inject
from ...models import Workflow
from ...models import WorkflowTask
from .._common import get_workflow_file_paths
from .._common import recursive_task_submission
from ..common import async_wrap
from ..common import TaskParameters
from .executor import FractalSlurmExecutor


"""
Slurm Bakend

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""


class SlurmConfig(BaseModel):
    """
    NOTE: We avoid calling it executor to avoid confusion with
    `concurrent.futures.Executor`
    """

    partition: str
    time: Optional[str]
    mem: Optional[str]
    nodes: Optional[str]
    ntasks_per_node: Optional[str]
    cpus_per_task: Optional[str]
    account: Optional[str]
    extra_lines: Optional[List[str]] = None

    def to_sbatch(self, prefix="#SBATCH "):
        dic = self.dict(exclude_none=True)
        sbatch_lines = []
        for k, v in dic.items():
            sbatch_lines.append(f"{prefix}--{k.replace('_', '-')}={v}")
        if self.extra_lines:
            sbatch_lines.extend(self.extra_lines)
        return sbatch_lines


def load_slurm_config(
    config_path: Optional[Path] = None,
) -> Dict[str, SlurmConfig]:
    """
    Parse slurm configuration
    """
    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE
    try:
        with config_path.open("r") as f:  # type: ignore
            raw_data = json.load(f)

        # coerce
        config_dict = {}
        for config_key in raw_data:
            config_dict[config_key] = SlurmConfig(**raw_data[config_key])
    except FileNotFoundError:
        raise SlurmConfigError(f"Configuration file not found: {config_path}")
    except Exception as e:
        raise SlurmConfigError(
            f"Could not read slurm configuration file: {config_path}"
            f"\nOriginal error: {repr(e)}"
        )
    return config_dict


class SlurmConfigError(ValueError):
    pass


def set_slurm_config(
    task: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
) -> Dict[str, Any]:
    """
    Collect slurm configuration parameters

    For now, this is the reference implementation for argument
    `submit_setup_call` of `runner._common.recursive_task_submission`

    Args:
        task:
            The task for which the sbatch script is to be assembled
        task_pars:
            The task parameters to be passed to the task
        workflow_dir:
            The directory in which the executor should store input / output /
            errors from task execution, as well as meta files from the
            submission process.

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
    worker_init: Optional[str] = None,
) -> Dict[str, Any]:
    """
    TODO:
    in case of failure we must return the most recent clean metadata

    Returns:
    output_dataset_metadata (Dict):
        the output metadata
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
