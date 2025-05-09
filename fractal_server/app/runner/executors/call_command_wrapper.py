import os
import shutil
import subprocess  # nosec
from shlex import split

from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.string_tools import validate_cmd


def call_command_wrapper(cmd: str, log_path: str) -> None:
    """
    Call a command and write its stdout and stderr to files

    Raises:
        TaskExecutionError: If the `subprocess.run` call returns a positive
                            exit code
        JobExecutionError:  If the `subprocess.run` call returns a negative
                            exit code (e.g. due to the subprocess receiving a
                            TERM or KILL signal)
    """
    try:
        validate_cmd(cmd)
    except ValueError as e:
        raise TaskExecutionError(f"Invalid command. Original error: {str(e)}")

    # Verify that task command is executable
    if shutil.which(split(cmd)[0]) is None:
        msg = (
            f'Command "{split(cmd)[0]}" is not valid. '
            "Hint: make sure that it is executable."
        )
        raise TaskExecutionError(msg)

    with open(log_path, "w") as fp_log:
        try:
            result = subprocess.run(  # nosec
                split(cmd),
                stderr=fp_log,
                stdout=fp_log,
            )
        except Exception as e:
            raise e

    if result.returncode != 0:
        if os.path.isfile(log_path):
            with open(log_path, "r") as fp_stderr:
                err = fp_stderr.read()
        else:
            err = ""
        raise TaskExecutionError(
            f"Task failed with returncode={result.returncode}.\nSTDERR: {err}"
        )
