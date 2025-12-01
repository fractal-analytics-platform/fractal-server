from typing import Any

import pytest

from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.executors.local.get_local_config import (
    get_local_backend_config,
)


def test_get_local_backend_config():
    class WorkflowTask(object):
        meta_parallel: dict[str, str] = {"parallel_tasks_per_job": 11}
        meta_non_parallel: dict[str, Any] = {"parallel_tasks_per_job": 22}

    wftask = WorkflowTask()
    shared_config = JobRunnerConfigLocal()

    out = get_local_backend_config(
        shared_config=shared_config,
        wftask=wftask,
        which_type="parallel",
    )
    assert out == JobRunnerConfigLocal(parallel_tasks_per_job=11)

    out = get_local_backend_config(
        shared_config=shared_config,
        wftask=wftask,
        which_type="non_parallel",
    )
    assert out == JobRunnerConfigLocal(parallel_tasks_per_job=22)

    with pytest.raises(ValueError):
        get_local_backend_config(
            shared_config=shared_config,
            wftask=wftask,
            which_type="invalid",
        )
