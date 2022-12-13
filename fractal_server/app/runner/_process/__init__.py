"""
Process Bakend

This backend runs fractal workflows as separate processes using a python
thread process pool, where each thread is responsible for running a single
task in a subprocess.

Incidentally, it represents the reference implementation for a backend.
"""
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
    Internal processing routine

    Schedules the workflow using a ThreadPoolExecutor.

    Cf. [process_workflow][fractal_server.app.runner._process.process_workflow]
    for the call signature.
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
    """
    Process workflow

    This function is responsible for running a workflow on some input data,
    saving the output and taking care of any exception raised during the run.

    NOTE: This is the `process` backend's public interface, which also works as
    a reference implementation for other backends.

    Args:
        workflow:
            The workflow to be run
        input_paths:
            The paths to the input files to pass to the first task of the
            workflow
        output_path:
            The destination path for the last task of the workflow
        input_metadata:
            Initial metadata, passed to the first task
        logger_name:
            Name of the logger to log information on the run to
        workflow_dir:
            Working directory for this run
        username:
            Username to impersonate to run the workflow
        worker_init:
            Any additional, usually backend specific, information to be passed
            to the backend executor.

    Raises:
        TaskExecutionError:
            Wrapper for errors raised by the tasks' executors.

    Returns:
        output_dataset_metadata:
            The updated metadata for the dataset, as returned by the last task
            of the workflow
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
