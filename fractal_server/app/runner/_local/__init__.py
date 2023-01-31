"""
Local Bakend

This backend runs Fractal workflows using python
[ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)
to run tasks in several threads. Incidentally, it also represents the reference
implementation for a backend.
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
) -> Dict[str, Any]:
    """
    Internal processing routine

    Schedules the workflow using a ThreadPoolExecutor.

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]
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
            ),
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir,
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
    Run a workflow

    This function is responsible for running a workflow on some input data,
    saving the output and taking care of any exception raised during the run.

    NOTE: This is the `local` backend's public interface, which also works as
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
            Working directory for this run.
        workflow_dir_user:
            Working directory for this run, on the user side. This argument is
            present for compatibility with the standard backend interface, but
            for the `local` backend it cannot be different from `workflow_dir`.
        slurm_user:
            Username to impersonate to run the workflow. This argument is
            present for compatibility with the standard backend interface, but
            is ignored in the `local` backend.
        worker_init:
            Any additional, usually backend specific, information to be passed
            to the backend executor. This argument is present for compatibility
            with the standard backend interface, but is ignored in the `local`
            backend.

    Raises:
        TaskExecutionError: wrapper for errors raised during tasks' execution
                            (positive exit codes).
        JobExecutionError: wrapper for errors raised by the tasks' executors
                           (negative exit codes).

    Returns:
        output_dataset_metadata:
            The updated metadata for the dataset, as returned by the last task
            of the workflow
    """

    if workflow_dir_user and (workflow_dir_user != workflow_dir):
        raise NotImplementedError(
            "Local backend does not support different directories "
            f"{workflow_dir=} and {workflow_dir_user=}"
        )

    output_dataset_metadata = await async_wrap(_process_workflow)(
        workflow=workflow,
        input_paths=input_paths,
        output_path=output_path,
        input_metadata=input_metadata,
        logger_name=logger_name,
        workflow_dir=workflow_dir,
    )
    return output_dataset_metadata
