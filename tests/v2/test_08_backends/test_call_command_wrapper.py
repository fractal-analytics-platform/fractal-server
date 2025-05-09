import pytest

from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.call_command_wrapper import (
    call_command_wrapper,
)


def test_call_command_wrapper(tmp_path):
    with pytest.raises(
        TaskExecutionError,
        match="Invalid command",
    ):
        call_command_wrapper(
            "echo; echo",
            log_path=(tmp_path / "log").as_posix(),
        )

    with pytest.raises(
        TaskExecutionError,
        match="is executable",
    ):
        call_command_wrapper(
            "xxxx something",
            log_path=(tmp_path / "log").as_posix(),
        )

    with pytest.raises(
        TaskExecutionError,
        match="Task failed with returncode=1",
    ):
        call_command_wrapper("false", log_path=(tmp_path / "log").as_posix())

    with pytest.raises(TaskExecutionError, match="unrecognized option"):
        call_command_wrapper(
            "sleep --fake-arg",
            log_path=(tmp_path / "log").as_posix(),
        )
