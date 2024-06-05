"""
Submodule to define _local_submit_setup
"""
from pathlib import Path
from typing import Literal
from typing import Optional

from ....models.v2 import WorkflowTaskV2
from ._local_config import get_local_backend_config


def _local_submit_setup(
    *,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Optional[Path] = None,
    workflow_dir_remote: Optional[Path] = None,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, object]:
    """
    Collect WorfklowTask-specific configuration parameters from different
    sources, and inject them for execution.

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
            `FractalProcessPoolExecutor.submit` and
            `FractalProcessPoolExecutor.map`, so as to set extra options.
    """

    local_backend_config = get_local_backend_config(
        wftask=wftask, which_type=which_type
    )

    return dict(local_backend_config=local_backend_config)
