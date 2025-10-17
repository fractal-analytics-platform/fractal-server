from pathlib import Path

from ..executors.local.get_local_config import get_local_backend_config
from ..executors.local.runner import LocalRunner
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.types import AttributeFilters


def process_workflow(
    *,
    job_id: int,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path | None = None,
    first_task_index: int | None = None,
    last_task_index: int | None = None,
    logger_name: str,
    job_attribute_filters: AttributeFilters,
    job_type_filters: dict[str, bool],
    user_id: int,
    resource: Resource,
    profile: Profile,
    user_cache_dir: str | None = None,
    fractal_ssh: FractalSSH | None = None,
    slurm_account: str | None = None,
    worker_init: str | None = None,
) -> None:
    """
    Run a workflow through a local backend.

    Args:
        job_id: Job ID.
        workflow: Workflow to be run
        dataset: Dataset to be used.
        workflow_dir_local: Local working directory for this job.
        workflow_dir_remote:
            Remote working directory for this job - only relevant for
            `slurm_sudo` and `slurm_ssh` backends.
        first_task_index:
            Positional index of the first task to execute; if `None`, start
            from `0`.
        last_task_index:
            Positional index of the last task to execute; if `None`, proceed
            until the last task.
        logger_name: Logger name
        user_id: User ID.
        resource: Computational resource for running this job.
        profile: Computational profile for running this job.
        user_cache_dir:
            User-writeable folder (typically a subfolder of `project_dir`).
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
        fractal_ssh:
            `FractalSSH` object, only relevant for the `slurm_ssh` backend.
        slurm_account:
            SLURM account to set.
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
        worker_init:
            Additional preamble lines for SLURM submission script.
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
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

    with LocalRunner(
        root_dir_local=workflow_dir_local,
        resource=resource,
        profile=profile,
    ) as runner:
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
