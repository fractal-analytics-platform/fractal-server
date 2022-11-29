from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from ...models import Workflow
from .._common import recursive_task_submission
from ..common import async_wrap
from ..common import TaskParameters


"""
Process Bakend

This backend runs fractal workflows as separate processes using a python
thread process pool, where each thread is responsible for running a single
task in a subprocess.

Incidentally, it represents the reference implementation for a backend.
"""


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

    with ThreadPoolExecutor() as executor:
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
