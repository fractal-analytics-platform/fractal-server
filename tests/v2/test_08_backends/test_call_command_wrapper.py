import pytest

from fractal_server.runner.exceptions import TaskExecutionError
from fractal_server.runner.executors.call_command_wrapper import (
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
            user_cache_dir="/something-invalid",
        )

    # Non-executable command
    with pytest.raises(
        TaskExecutionError,
        match="is executable",
    ):
        call_command_wrapper(
            cmd="xxxx something",
            log_path=(tmp_path / "log2").as_posix(),
            user_cache_dir="/something-invalid",
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
            user_cache_dir="/something-invalid",
        )

    # Command that actually fails with returncode!=0
    # The error message depends on the OS. The following works on Mac and Linux
    with pytest.raises(
        TaskExecutionError,
        match="(unrecognized|illegal) option",
    ):
        call_command_wrapper(
            cmd="sleep --fake-arg",
            log_path=(tmp_path / "log4").as_posix(),
            user_cache_dir="/something-invalid",
        )


def test_call_command_wrapper_FRACTAL_CACHE_DIR(tmp_path):
    user_cache_dir = "/some/fractal/cache/dir"

    # Example of how `-u` makes the subprocess fail for an undefined variable
    script_path = (tmp_path / "script1.sh").as_posix()
    logfile = (tmp_path / "log1").as_posix()
    with open(script_path, "w") as f:
        f.write('set -u; echo "$MISSING_VARIABLE;"')
    with pytest.raises(TaskExecutionError, match="unbound variable"):
        call_command_wrapper(
            cmd=f"bash {script_path}",
            log_path=logfile,
            user_cache_dir=user_cache_dir,
        )

    # Successful execution and reference to `FRACTAL_CACHE_DIR`
    script_path = (tmp_path / "script2.sh").as_posix()
    logfile = (tmp_path / "log2").as_posix()
    with open(script_path, "w") as f:
        f.write('set -u; echo "FRACTAL_CACHE_DIR=$FRACTAL_CACHE_DIR;"')
    call_command_wrapper(
        cmd=f"bash {script_path}",
        log_path=logfile,
        user_cache_dir=user_cache_dir,
    )
    with open(logfile) as f:
        assert f"FRACTAL_CACHE_DIR={user_cache_dir}" in f.read()


def test_call_command_wrapper_unreachable_branch(tmp_path):
    with pytest.raises(
        TypeError,
        match="expected str, bytes or os.PathLike object, not list",
    ):
        call_command_wrapper(
            cmd="ls",
            log_path=(tmp_path / "log").as_posix(),
            user_cache_dir=["/something"],
        )
