from pathlib import Path

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.runner.executors.local.get_local_config import (
    get_local_backend_config,
)
from fractal_server.runner.v2.runner import execute_tasks


def execute_tasks_mod(
    wf_task_list: list[WorkflowTaskV2],
    workflow_dir_local: Path,
    user_id: int,
    job_id: int,
    job_attribute_filters: dict[str, bool] | None = None,
    job_type_filters: dict[str, bool] | None = None,
    **kwargs,
) -> None:
    """
    This is a version of `execute_tasks` with some defaults pre-filled.
    """
    execute_tasks(
        wf_task_list=wf_task_list,
        workflow_dir_local=workflow_dir_local,
        job_attribute_filters=(job_attribute_filters or {}),
        job_type_filters=(job_type_filters or {}),
        job_id=job_id,
        user_id=user_id,
        get_runner_config=get_local_backend_config,
        **kwargs,
    )
