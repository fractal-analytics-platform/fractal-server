from pathlib import Path
from typing import Any
from typing import Literal

from ...task_files import TaskFiles
from ._local_config import get_local_backend_config
from fractal_server.app.models.v2 import WorkflowTaskV2


def _local_submit_setup(
    *,
    wftask: WorkflowTaskV2,
    root_dir_local: Path,
    root_dir_remote: Path,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, Any]:
    """
    Collect WorkflowTask-specific configuration parameters from different
    sources, and inject them for execution.

    FIXME

    Arguments:
        wftask:
            WorkflowTask for which the configuration is to be assembled
        workflow_dir_local:
            Not used in this function.
        workflow_dir_remote:
            Not used in this function.

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalThreadPoolExecutor.submit` and
            `FractalThreadPoolExecutor.map`, so as to set extra options.
    """

    local_backend_config = get_local_backend_config(
        wftask=wftask,
        which_type=which_type,
    )

    # Get TaskFiles object
    task_files = TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
    )

    return dict(
        local_backend_config=local_backend_config,
        task_files=task_files,
    )
