from typing import Any
from typing import Literal

from ._local_config import get_local_backend_config
from fractal_server.app.models.v2 import WorkflowTaskV2


# FIXME: can we drop the "xxx_submit_setup" layer and go directly
# with "get_xxx_config"


def _local_submit_setup(
    *,
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, Any]:
    """
    Collect WorkflowTask-specific configuration parameters from different
    sources, and inject them for execution.

    FIXME

    Arguments:
        wftask: WorkflowTask for which the configuration is to be assembled
        which_type: Whether it is a parallel or non-parallel task.
    """

    local_backend_config = get_local_backend_config(
        wftask=wftask, which_type=which_type
    )

    return dict(local_backend_config=local_backend_config)
