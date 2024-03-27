"""
Homogeneous set of Python functions that wrap executable commands.
Each function in this module should have
1. An input argument `input_kwargs`
2. An input argument `task`
3. A `dict[str, Any]` return type, that will be validated downstram with
    either TaskOutput or InitTaskOutput
... TBD
"""
import json
import logging
import shutil
import subprocess  # nosec
from pathlib import Path
from shlex import split as shlex_split
from typing import Any
from typing import Optional

from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from fractal_server.app.runner.task_files import get_task_file_paths
from fractal_server.app.runner.v2.models import WorkflowTask


def _call_command_wrapper(cmd: str, stdout: Path, stderr: Path) -> None:
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

    fp_stdout = open(stdout, "w")
    fp_stderr = open(stderr, "w")
    try:
        result = subprocess.run(  # nosec
            shlex_split(cmd),
            stderr=fp_stderr,
            stdout=fp_stdout,
        )
    except Exception as e:
        raise e
    finally:
        fp_stdout.close()
        fp_stderr.close()

    if result.returncode > 0:
        with stderr.open("r") as fp_stderr:
            err = fp_stderr.read()
        raise TaskExecutionError(err)
    elif result.returncode < 0:
        raise JobExecutionError(
            info=f"Task failed with returncode={result.returncode}"
        )


def _run_single_task(
    args: dict[str, Any],
    command: str,
    wftask: WorkflowTask,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
    is_task_v1: bool = False,
) -> dict[str, Any]:
    """
    Runs within an executor.
    """

    logger = logging.getLogger(logger_name)

    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    task_files = get_task_file_paths(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=wftask.order,
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
            stdout=task_files.out,
            stderr=task_files.err,
        )
    except TaskExecutionError as e:
        e.workflow_task_order = wftask.order
        e.workflow_task_id = wftask.id
        e.task_name = wftask.task.name
        raise e

    try:
        with task_files.metadiff.open("r") as f:
            out_meta = json.load(f)
    except FileExistsError as e:
        logger.warning(
            f"Task did not produce output metadata. Original error: {str(e)}"
        )
        out_meta = None

    if out_meta == {}:
        return None
    return out_meta
