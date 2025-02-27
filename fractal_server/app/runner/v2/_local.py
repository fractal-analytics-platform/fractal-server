from pathlib import Path
from typing import Optional

from ...models.v2 import DatasetV2
from ...models.v2 import WorkflowV2
from ..executors.local._submit_setup import _local_submit_setup
from ..executors.local.runner import LocalRunner
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
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
    user_id: int,
    **kwargs,
) -> None:
    """
    Run a workflow through

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
        user_id:

    Raises:
        TaskExecutionError: wrapper for errors raised during tasks' execution
                            (positive exit codes).
        JobExecutionError: wrapper for errors raised by the tasks' executors
                           (negative exit codes).
    """

    if workflow_dir_remote and (workflow_dir_remote != workflow_dir_local):
        raise NotImplementedError(
            "Local backend does not support different directories "
            f"{workflow_dir_local=} and {workflow_dir_remote=}"
        )

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    with LocalRunner(root_dir_local=workflow_dir_local) as runner:
        execute_tasks_v2(
            wf_task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)
            ],
            dataset=dataset,
            runner=runner,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_local,
            logger_name=logger_name,
            submit_setup_call=_local_submit_setup,
            job_attribute_filters=job_attribute_filters,
            user_id=user_id,
        )
