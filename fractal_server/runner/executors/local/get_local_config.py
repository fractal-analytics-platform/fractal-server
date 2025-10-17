"""
Submodule to handle the local-backend configuration for a WorkflowTask
"""
from typing import Literal

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.runner.config import JobRunnerConfigLocal


def get_local_backend_config(
    shared_config: JobRunnerConfigLocal,
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    tot_tasks: int = 1,
) -> JobRunnerConfigLocal:
    """
    Prepare a specific `LocalBackendConfig` configuration.

    The base configuration is the runner-level `shared_config` object, based
    on `resource.jobs_runner_config`. We then incorporate attributes from
    `wftask.meta_{non_parallel,parallel}` - with higher priority.

    Args:
        shared_config:
            Configuration object based on `resource.jobs_runner_config`.
        wftask:
            WorkflowTaskV2 for which the backend configuration should
            be prepared.
        which_type:
            Whether we should look at the non-parallel or parallel part
            of `wftask`.
        tot_tasks: Not used here, only present as a common interface.

    Returns:
        A ready-to-use local-backend configuration object.
    """

    if which_type == "non_parallel":
        wftask_meta = wftask.meta_non_parallel
    elif which_type == "parallel":
        wftask_meta = wftask.meta_parallel
    else:
        raise ValueError(
            f"Invalid {which_type=} in `get_local_backend_config`."
        )

    __KEY__ = "parallel_tasks_per_job"
    output = shared_config.model_copy(deep=True)
    if wftask_meta and __KEY__ in wftask_meta:
        output.parallel_tasks_per_job = wftask_meta[__KEY__]
    return output
