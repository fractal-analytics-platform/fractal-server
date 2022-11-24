from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from ...models import Workflow
from .._common import recursive_task_submission
from ..common import async_wrap
from ..common import TaskParameters
from .executor import FractalSlurmExecutor
from pydantic import BaseModel
from ....syringe import Inject
from ....config import get_settings
import json


"""
Slurm Bakend

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""


class SlurmConfig(BaseModel):
    """
    `name` maps on `Task.executor`.

    NOTE: We avoid calling it executor to avoid confusion with
    `concurrent.futures.Executor`
    """
    name: str
    partition: str
    time: Optional[str]
    mem: Optional[str]
    nodes: Optional[str]
    ntasks_per_node: Optional[str]
    cpus_per_task: Optional[str]
    nodes: Optional[str]
    account: Optional[str]
    extra_lines: Optional[List[str]] = None

    def to_sbatch(self, prefix="#SBATCH "):
        dic = self.dict(exclude_none=True, exclude={"name"})
        sbatch_lines = []
        for k, v in dic.items():
            sbatch_lines.append(f"{prefix}--{k.replace('_', '-')}={v}")
        if self.extra_lines:
            sbatch_lines.extend(self.extra_lines)
        return sbatch_lines


def load_slurm_config(config_path: Path) -> Dict[str, SlurmConfig]:
    """
    Parse slurm configuration
    """
    settings = Inject(get_settings)
    with settings.FRACTAL_SLURM_CONFIG_FILE.open("r") as f:
        raw_data = json.load(f)
    config_list = {item["name"]: SlurmConfig(**item) for item in raw_data}
    return config_list


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: List[Path],
    output_path: Path,
    input_metadata: Dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    username: str = None,
    worker_init: Optional[
        str
    ] = None,  # this is only to match to _parsl interface
) -> Dict[str, Any]:
    """
    TODO:
    in case of failure we must return the most recent clean metadata

    Returns:
    output_dataset_metadata (Dict):
        the output metadata
    """

    with FractalSlurmExecutor(
        debug=True,
        keep_logs=True,
        username=username,
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
    worker_init: Optional[
        str
    ] = None,  # this is only to match to _parsl interface
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
