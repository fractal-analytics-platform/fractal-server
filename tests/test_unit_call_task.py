from concurrent.futures import ThreadPoolExecutor

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._common import execute_tasks
from fractal_server.app.runner.common import TaskParameters


def wrong_submit_setup_call(*args, **kwargs):
    raise ValueError("custom error")


@pytest.mark.parametrize("is_parallel", [True, False])
def test_execute_tasks(is_parallel, tmp_path):
    """
    Check that an exception in `submit_setup_call` is handled correctly in
    `execute_tasks`, both for parallel and non-parallel tasks.
    """
    if is_parallel:
        task = MockTask(
            name="name",
            source="source",
            command="command",
            parallelization_level="component",
        )
    else:
        task = MockTask(
            name="name",
            source="source",
            command="command",
        )
    debug(is_parallel)
    debug(task.parallelization_level)

    task_list = [MockWorkflowTask(task=task)]
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata=dict(
            component=["some_item"],
        ),
    )
    with ThreadPoolExecutor() as executor:
        with pytest.raises(RuntimeError) as e:
            execute_tasks(
                executor=executor,
                task_list=task_list,
                task_pars=task_pars,
                workflow_dir=tmp_path,
                submit_setup_call=wrong_submit_setup_call,
                logger_name="logger",
            )
        debug(e.value)
        assert "error in submit_setup_call=" in str(e.value)
        assert 'ValueError("custom error")' in str(e.value)
