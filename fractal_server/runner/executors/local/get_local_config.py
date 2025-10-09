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
    Prepare a specific `LocalBackendConfig` configuration object

    The sources for `parallel_tasks_per_job` attributes, starting from the
    highest-priority one, are

    1. Properties in `wftask.meta_parallel` or `wftask.meta_non_parallel`
       (depending on `which_type`);
    2. The runner-level `shared_config` object.

    # FIXME (zzz): docstring & arguments

    Arguments:
        wftask:
            WorkflowTaskV2 for which the backend configuration should
            be prepared.

    Returns:
        A local-backend configuration object
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
    output = JobRunnerConfigLocal(**shared_config.model_dump())
    if wftask_meta and __KEY__ in wftask_meta:
        output.parallel_tasks_per_job = wftask_meta[__KEY__]
    return output
