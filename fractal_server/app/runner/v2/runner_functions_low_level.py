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
from fractal_server.app.runner.task_files import get_task_file_paths


def _call_command_wrapper(cmd: str, log_path: Path) -> None:
    """
    Call a command and write its stdout and stderr to files

    Raises:
        TaskExecutionError: If the `subprocess.run` call returns a positive
                            exit code
        JobExecutionError:  If the `subprocess.run` call returns a negative
                            exit code (e.g. due to the subprocess receiving a
                            TERM or KILL signal)
    """

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
        with log_path.open("r") as fp_stderr:
            err = fp_stderr.read()
        raise TaskExecutionError(err)
    elif result.returncode < 0:
        raise JobExecutionError(
            info=f"Task failed with returncode={result.returncode}"
        )


def run_single_task(
    args: dict[str, Any],
    command: str,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
    is_task_v1: bool = False,
) -> dict[str, Any]:
    """
    Runs within an executor.
    """

    logger = logging.getLogger(logger_name)
    logger.debug(f"Now start running {command=}")

    if not workflow_dir_remote:
        workflow_dir_remote = workflow_dir_local

    if is_task_v1:
        task_name = wftask.task_legacy.name
    else:
        task_name = wftask.task.name

    component = args.pop(_COMPONENT_KEY_, None)
    task_files = get_task_file_paths(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=task_name,
        component=component,
    )

    # Write arguments to args.json file
    with task_files.args.open("w") as f:
        json.dump(args, f, indent=2)

    # Assemble full command
    if is_task_v1:
        full_command = (
            f"{command} "
            f"--json {task_files.args.as_posix()} "
            f"--metadata-out {task_files.metadiff.as_posix()}"
        )
    else:
        full_command = (
            f"{command} "
            f"--args-json {task_files.args.as_posix()} "
            f"--out-json {task_files.metadiff.as_posix()}"
        )

    try:
        _call_command_wrapper(
            full_command,
            log_path=task_files.log,
        )
    except TaskExecutionError as e:
        e.workflow_task_order = wftask.order
        e.workflow_task_id = wftask.id
        if wftask.is_legacy_task:
            e.task_name = wftask.task_legacy.name
        else:
            e.task_name = wftask.task.name
        raise e

    try:
        with task_files.metadiff.open("r") as f:
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
