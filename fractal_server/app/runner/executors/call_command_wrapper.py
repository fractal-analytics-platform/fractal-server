import os
import shlex
import shutil
import subprocess  # nosec

from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.string_tools import validate_cmd


def call_command_wrapper(*, cmd: str, log_path: str) -> None:
    """
    Call a command and write its stdout and stderr to files

    Args:
        cmd:
        log_path:
    """
    try:
        validate_cmd(cmd)
    except ValueError as e:
        raise TaskExecutionError(f"Invalid command. Original error: {str(e)}")

    split_cmd = shlex.split(cmd)

    # Verify that task command is executable
    if shutil.which(split_cmd[0]) is None:
        msg = (
            f'Command "{split_cmd[0]}" is not valid. '
            "Hint: make sure that it is executable."
        )
        raise TaskExecutionError(msg)

    with open(log_path, "w") as fp_log:
        try:
            result = subprocess.run(  # nosec
                split_cmd,
                stderr=fp_log,
                stdout=fp_log,
            )
        except Exception as e:
            # This is likely unreachable
            raise e

    if result.returncode != 0:
        stderr = ""
        if os.path.isfile(log_path):
            with open(log_path) as fp_stderr:
                stderr = fp_stderr.read()
        raise TaskExecutionError(
            f"Task failed with returncode={result.returncode}.\n"
            f"STDERR: {stderr}"
        )
