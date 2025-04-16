import json
import logging
import shutil
import subprocess  # nosec
from shlex import split
from typing import Any

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.string_tools import validate_cmd


def _call_command_wrapper(cmd: str, log_path: str) -> None:
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

    if result.returncode > 0:
        with open(log_path, "r") as fp_stderr:
            err = fp_stderr.read()
        raise TaskExecutionError(err)
    elif result.returncode < 0:
        raise JobExecutionError(
            info=f"Task failed with returncode={result.returncode}"
        )


def run_single_task(
    # COMMON to all parallel tasks
    command: str,
    workflow_task_order: int,
    workflow_task_id: int,
    task_name: str,
    # SPECIAL for each parallel task
    parameters: dict[str, Any],
    remote_files: dict[str, str],
) -> dict[str, Any]:
    """
    Runs within an executor (AKA on the SLURM cluster).
    """

    try:
        args_file_remote = remote_files["args_file_remote"]
        metadiff_file_remote = remote_files["metadiff_file_remote"]
        log_file_remote = remote_files["log_file_remote"]
    except KeyError:
        raise TaskExecutionError(
            f"Invalid {remote_files=}",
            workflow_task_order=workflow_task_order,
            workflow_task_id=workflow_task_id,
            task_name=task_name,
        )

    logger = logging.getLogger(None)
    logger.debug(f"Now start running {command=}")

    # Write arguments to args.json file
    # NOTE: see issue 2346
    with open(args_file_remote, "w") as f:
        json.dump(parameters, f, indent=2)

    # Assemble full command
    # NOTE: this could be assembled backend-side
    full_command = (
        f"{command} "
        f"--args-json {args_file_remote} "
        f"--out-json {metadiff_file_remote}"
    )

    try:
        _call_command_wrapper(
            full_command,
            log_path=log_file_remote,
        )
    except TaskExecutionError as e:
        e.workflow_task_order = workflow_task_order
        e.workflow_task_id = workflow_task_id
        e.task_name = task_name
        raise e

    try:
        with open(metadiff_file_remote, "r") as f:
            out_meta = json.load(f)
    except FileNotFoundError as e:
        logger.debug(
            "Task did not produce output metadata. "
            f"Original FileNotFoundError: {str(e)}"
        )
        out_meta = None

    if out_meta == {}:
        return None
    return out_meta
