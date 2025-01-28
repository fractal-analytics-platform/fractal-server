from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Optional

from ....models.v2 import DatasetV2
from ....models.v2 import WorkflowV2
from ...exceptions import JobExecutionError
from ...filenames import SHUTDOWN_FILENAME
from ...set_start_and_last_task_index import set_start_and_last_task_index
from ..runner import execute_tasks_v2
from ._submit_setup import _local_submit_setup
from .executor import FractalProcessPoolExecutor
from fractal_server.images.models import AttributeFiltersType


def process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
    logger_name: str,
    job_attribute_filters: AttributeFiltersType,
    # Slurm-specific
    user_cache_dir: Optional[str] = None,
    slurm_user: Optional[str] = None,
    slurm_account: Optional[str] = None,
    worker_init: Optional[str] = None,
) -> None:
    """
    Run a workflow

    This function is responsible for running a workflow on some input data,
    saving the output and taking care of any exception raised during the run.

    NOTE: This is the `local_experimental` backend's public interface,
    which also works as a reference implementation for other backends.

    Args:
        workflow:
            The workflow to be run
        dataset:
            Initial dataset.
        workflow_dir_local:
            Working directory for this run.
        workflow_dir_remote:
            Working directory for this run, on the user side. This argument is
            present for compatibility with the standard backend interface, but
            for the `local` backend it cannot be different from
            `workflow_dir_local`.
        first_task_index:
            Positional index of the first task to execute; if `None`, start
            from `0`.
        last_task_index:
            Positional index of the last task to execute; if `None`, proceed
            until the last task.
        logger_name: Logger name
        slurm_user:
            Username to impersonate to run the workflow. This argument is
            present for compatibility with the standard backend interface, but
            is ignored in the `local` backend.
        slurm_account:
            SLURM account to use when running the workflow. This argument is
            present for compatibility with the standard backend interface, but
            is ignored in the `local` backend.
        user_cache_dir:
            Cache directory of the user who will run the workflow. This
            argument is present for compatibility with the standard backend
            interface, but is ignored in the `local` backend.
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
    """

    if workflow_dir_remote and (workflow_dir_remote != workflow_dir_local):
        raise NotImplementedError(
            "LocalExperimental backend does not support different directories "
            f"{workflow_dir_local=} and {workflow_dir_remote=}"
        )

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    with FractalProcessPoolExecutor(
        shutdown_file=workflow_dir_local / SHUTDOWN_FILENAME
    ) as executor:
        try:
            execute_tasks_v2(
                wf_task_list=workflow.task_list[
                    first_task_index : (last_task_index + 1)
                ],
                dataset=dataset,
                executor=executor,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_local,
                logger_name=logger_name,
                submit_setup_call=_local_submit_setup,
                job_attribute_filters=job_attribute_filters,
            )
        except BrokenProcessPool as e:
            raise JobExecutionError(
                info=(
                    "Job failed with BrokenProcessPool error, likely due to "
                    f"an executor shutdown.\nOriginal error:\n{e.args[0]}"
                )
            )
