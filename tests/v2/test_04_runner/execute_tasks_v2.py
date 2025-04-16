from pathlib import Path
from typing import Optional

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.executors.local.get_local_config import (
    get_local_backend_config,
)
from fractal_server.app.runner.v2.runner import execute_tasks_v2


def execute_tasks_v2_mod(
    wf_task_list: list[WorkflowTaskV2],
    workflow_dir_local: Path,
    user_id: int,
    job_type_filters: Optional[dict[str, bool]] = None,
    **kwargs,
) -> None:
    """
    This is a version of `execute_tasks_v2` with some defaults pre-filled.
    """
    execute_tasks_v2(
        wf_task_list=wf_task_list,
        workflow_dir_local=workflow_dir_local,
        job_attribute_filters={},
        job_type_filters=(job_type_filters or {}),
        user_id=user_id,
        get_runner_config=get_local_backend_config,
        **kwargs,
    )
