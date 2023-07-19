from concurrent.futures import ThreadPoolExecutor

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._common import call_parallel_task
from fractal_server.app.runner.common import TaskParameters


def wrong_submit_setup_call(*args, **kwargs):
    raise ValueError("custom error")


def test_call_parallel_task_with_wrong_submit_setup_call(tmp_path):
    """
    Check that an exception in `submit_setup_call` is handled correctly in
    `call_parallel_task`.
    """
    wftask = MockWorkflowTask(
        task=MockTask(
            name="name",
            source="source",
            command="command",
            parallelization_level="component",
        )
    )
    task_pars_depend = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata=dict(
            component=["some_item"],
        ),
    )
    with ThreadPoolExecutor() as executor:
        with pytest.raises(RuntimeError) as e:
            call_parallel_task(
                executor=executor,
                wftask=wftask,
                task_pars_depend=task_pars_depend,
                workflow_dir=tmp_path,
                submit_setup_call=wrong_submit_setup_call,
            )
        debug(e.value)
        assert "error in submit_setup_call=" in str(e.value)
