import pytest

from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.v2._local.executor import (
    FractalThreadPoolExecutor,
)
from fractal_server.app.runner.v2.runner_functions_low_level import (
    _call_command_wrapper,
)


async def test_command_wrapper(tmp_path):
    with FractalThreadPoolExecutor() as executor:
        future1 = executor.submit(
            _call_command_wrapper, "ls -al .", log_path="/tmp/fractal_log"
        )
        future2 = executor.submit(
            _call_command_wrapper, "ls -al ./*", log_path="/tmp/fractal_log"
        )

    future1.result()
    with pytest.raises(TaskExecutionError) as e:
        future2.result()
    assert "must not contain any of this characters" in e._excinfo[1].args[0]
