import json

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._local._local_config import (
    get_local_backend_config,
)
from fractal_server.app.runner._local._local_config import (
    LocalBackendConfigError,
)


def test_get_local_backend_config(tmp_path):
    """
    Testing that:
    1. WorkflowTask.meta overrides WorkflowTask.task.meta;
    2. WorkflowTask.task.meta overrides global config.
    """

    CONFIG_VALUE = 2
    TASK_VALUE = 3
    WFTASK_VALUE = 4

    # Global configuration
    config = dict(parallel_tasks_per_job=CONFIG_VALUE)
    config_file = tmp_path / "config.json"
    with config_file.open("w") as f:
        json.dump(config, f)

    # Case 1: no set value
    mytask = MockTask(name="T", command="cmd", meta={})
    mywftask = MockWorkflowTask(task=mytask, meta={})
    debug(mywftask.meta)
    local_backend_config = get_local_backend_config(wftask=mywftask)
    assert local_backend_config.parallel_tasks_per_job is None

    # Case 2: highest-priority set value is in config file
    mytask = MockTask(name="T", command="cmd", meta={})
    mywftask = MockWorkflowTask(task=mytask, meta={})
    debug(mywftask.meta)
    local_backend_config = get_local_backend_config(
        wftask=mywftask,
        config_path=config_file,
    )
    assert local_backend_config.parallel_tasks_per_job == CONFIG_VALUE

    # Case 3: highest-priority set value is in Task
    task_meta = dict(parallel_tasks_per_job=TASK_VALUE)
    mytask = MockTask(name="T", command="cmd", meta=task_meta)
    mywftask = MockWorkflowTask(task=mytask, meta={})
    debug(mywftask.meta)
    local_backend_config = get_local_backend_config(
        wftask=mywftask,
        config_path=config_file,
    )
    assert local_backend_config.parallel_tasks_per_job == TASK_VALUE

    # Case 4: highest-priority set value is in WorfklowTask
    wftask_meta = dict(parallel_tasks_per_job=WFTASK_VALUE)
    mytask = MockTask(name="T", command="cmd", meta=task_meta)
    mywftask = MockWorkflowTask(task=mytask, meta=wftask_meta)
    debug(mywftask.meta)
    local_backend_config = get_local_backend_config(
        wftask=mywftask,
        config_path=config_file,
    )
    assert local_backend_config.parallel_tasks_per_job == WFTASK_VALUE


def test_get_local_backend_config_fail(tmp_path):

    # Global configuration
    config = dict(parallel_tasks_per_job=1, invalid_key=0)
    config_file = tmp_path / "config.json"
    debug(config_file)
    with config_file.open("w") as f:
        json.dump(config, f)

    # Fail
    mytask = MockTask(name="T", command="cmd", meta={})
    mywftask = MockWorkflowTask(task=mytask, meta={})
    with pytest.raises(LocalBackendConfigError):
        _ = get_local_backend_config(wftask=mywftask, config_path=config_file)
