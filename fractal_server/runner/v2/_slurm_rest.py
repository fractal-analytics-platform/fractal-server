"""
SLURM REST backend (prototype)

Same as `_slurm_ssh.py`, but with `SlurmRestRunner`. Selected in
`submit_workflow.py` when `FRACTAL_SLURM_REST_ENABLED=true`.
"""

from pathlib import Path

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.runner.executors.slurm_common.get_slurm_config import (
    get_slurm_config,
)
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.set_start_and_last_task_index import (
    set_start_and_last_task_index,
)
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.types import AttributeFilters

from ._worker_init import _get_worker_init_lines
from .runner import execute_tasks


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
    fractal_ssh: FractalSSH | None = None,
    slurm_account: str | None = None,
    worker_init: str | None = None,
    user_cache_dir: str,
) -> None:
    """
    Run a workflow through the SLURM REST runner.

    Arguments are the same as for `_slurm_ssh.process_workflow`.
    """
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    with SlurmRestRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        slurm_account=slurm_account,
        resource=resource,
        profile=profile,
        common_script_lines=_get_worker_init_lines(worker_init),
        user_cache_dir=user_cache_dir,
        fractal_job_id=job_id,
        resource_id=resource.id,
    ) as runner:
        execute_tasks(
            wf_task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)
            ],
            dataset=dataset,
            job_id=job_id,
            runner=runner,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            logger_name=logger_name,
            get_runner_config=get_slurm_config,
            job_attribute_filters=job_attribute_filters,
            job_type_filters=job_type_filters,
            user_id=user_id,
            resource_id=resource.id,
        )
