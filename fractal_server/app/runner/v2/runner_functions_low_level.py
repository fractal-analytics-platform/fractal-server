import json
import logging
import shutil
import subprocess  # nosec
from pathlib import Path
from shlex import split as shlex_split
from typing import Any
from typing import Optional

from ..components import _COMPONENT_KEY_
from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.task_files import TaskFiles
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
    if shutil.which(shlex_split(cmd)[0]) is None:
        msg = (
            f'Command "{shlex_split(cmd)[0]}" is not valid. '
            "Hint: make sure that it is executable."
        )
        raise TaskExecutionError(msg)

    with open(log_path, "w") as fp_log:
        try:
            result = subprocess.run(  # nosec
                shlex_split(cmd),
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
    parameters: dict[str, Any],
    command: str,
    wftask: WorkflowTaskV2,
    root_dir_local: Path,
    root_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
) -> dict[str, Any]:
    """
    Runs within an executor (AKA on the SLURM cluster).
    """

    logger = logging.getLogger(logger_name)
    logger.debug(f"Now start running {command=}")

    if not root_dir_remote:
        root_dir_remote = root_dir_local

    task_name = wftask.task.name

    component = parameters.pop(_COMPONENT_KEY_)
    task_files = TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_remote,
        task_name=task_name,
        task_order=wftask.order,
        component=component,
    )

    # Write arguments to args.json file
    with open(task_files.args_file_remote, "w") as f:
        json.dump(parameters, f, indent=2)

    # Assemble full command
    full_command = (
        f"{command} "
        f"--args-json {task_files.args_file_remote} "
        f"--out-json {task_files.metadiff_file_remote}"
    )

    try:
        _call_command_wrapper(
            full_command,
            log_path=task_files.log_file_remote,
        )
    except TaskExecutionError as e:
        e.workflow_task_order = wftask.order
        e.workflow_task_id = wftask.id
        e.task_name = wftask.task.name
        raise e

    try:
        with open(task_files.metadiff_file_remote, "r") as f:
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
