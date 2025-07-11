import pytest

from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.call_command_wrapper import (
    call_command_wrapper,
)


def test_call_command_wrapper(tmp_path):
    # Invalid command
    with pytest.raises(
        TaskExecutionError,
        match="Invalid command",
    ):
        call_command_wrapper(
            cmd="echo; echo",
            log_path=(tmp_path / "log1").as_posix(),
        )

    # Non-executable command
    with pytest.raises(
        TaskExecutionError,
        match="is executable",
    ):
        call_command_wrapper(
            cmd="xxxx something",
            log_path=(tmp_path / "log2").as_posix(),
        )

    # Internal failure (cannot call `shlex.split`)

    # Command that actually fails with returncode!=0
    with pytest.raises(
        TaskExecutionError,
        match="Task failed with returncode=1",
    ):
        call_command_wrapper(
            cmd="false",
            log_path=(tmp_path / "log3").as_posix(),
        )

    # Command that actually fails with returncode!=0
    with pytest.raises(
        TaskExecutionError,
        match="unrecognized option",
    ):
        call_command_wrapper(
            cmd="sleep --fake-arg",
            log_path=(tmp_path / "log4").as_posix(),
        )
