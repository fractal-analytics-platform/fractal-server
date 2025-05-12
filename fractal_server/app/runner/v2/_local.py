from pathlib import Path

from ...models.v2 import DatasetV2
from ...models.v2 import WorkflowV2
from ..executors.local.get_local_config import get_local_backend_config
from ..executors.local.runner import LocalRunner
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
from fractal_server.types import AttributeFilters


def process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    job_id: int,
    workflow_dir_remote: Path | None = None,
    first_task_index: int | None = None,
    last_task_index: int | None = None,
    logger_name: str,
    job_attribute_filters: AttributeFilters,
    job_type_filters: dict[str, bool],
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
            job_id=job_id,
            runner=runner,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_local,
            logger_name=logger_name,
            get_runner_config=get_local_backend_config,
            job_attribute_filters=job_attribute_filters,
            job_type_filters=job_type_filters,
            user_id=user_id,
        )
