"""
Common utilities and routines for runner backends (private API)

This module includes utilities and routines that are of use to implement
runner backends and that should not be exposed outside of the runner
subsystem.
"""
import json
import shutil
import subprocess  # nosec
import traceback
from concurrent.futures import Executor
from functools import lru_cache
from functools import partial
from pathlib import Path
from shlex import split as shlex_split
from typing import Callable
from typing import Optional

from ...logger import get_logger
from ..models import WorkflowTask
from ..models import WorkflowTaskStatusType
from .common import JobExecutionError
from .common import TaskExecutionError
from .common import TaskParameters
from .common import write_args_file


METADATA_FILENAME = "metadata.json"
SHUTDOWN_FILENAME = "shutdown"


def no_op_submit_setup_call(
    *,
    wftask: WorkflowTask,
    workflow_dir: Path,
    workflow_dir_user: Path,
    task_pars: TaskParameters,
) -> dict:
    """
    Default (no-operation) interface of submit_setup_call.
    """
    return {}


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


class TaskFiles:
    """
    Group all file paths pertaining to a task

    Attributes:
        workflow_dir:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_user:
            User-side directory with the same scope as `workflow_dir`, and
            where a user can write.
        task_order:
            Positional order of the task within a workflow.
        component:
            Specific component to run the task for (relevant for tasks that
            will be executed in parallel over many components).
        file_prefix:
            Prefix for all task-related files.
        args:
            Path for input json file.
        metadiff:
            Path for output json file with metadata update.
        out:
            Path for task-execution stdout.
        err:
            Path for task-execution stderr.
    """

    workflow_dir: Path
    workflow_dir_user: Path
    task_order: Optional[int] = None
    component: Optional[str] = None

    file_prefix: str
    args: Path
    out: Path
    err: Path
    metadiff: Path

    def __init__(
        self,
        workflow_dir: Path,
        workflow_dir_user: Path,
        task_order: Optional[int] = None,
        component: Optional[str] = None,
    ):
        self.workflow_dir = workflow_dir
        self.workflow_dir_user = workflow_dir_user
        self.task_order = task_order
        self.component = component

        if self.component is not None:
            component_safe = sanitize_component(str(self.component))
            component_safe = f"_par_{component_safe}"
        else:
            component_safe = ""

        if self.task_order is not None:
            order = str(self.task_order)
        else:
            order = "task"
        self.file_prefix = f"{order}{component_safe}"
        self.args = self.workflow_dir_user / f"{self.file_prefix}.args.json"
        self.out = self.workflow_dir_user / f"{self.file_prefix}.out"
        self.err = self.workflow_dir_user / f"{self.file_prefix}.err"
        self.metadiff = (
            self.workflow_dir_user / f"{self.file_prefix}.metadiff.json"
        )


@lru_cache()
def get_task_file_paths(
    workflow_dir: Path,
    workflow_dir_user: Path,
    task_order: Optional[int] = None,
    component: Optional[str] = None,
) -> TaskFiles:
    """
    Return the corrisponding TaskFiles object

    This function is mainly used as a cache to avoid instantiating needless
    objects.
    """
    return TaskFiles(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=task_order,
        component=component,
    )


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


def call_single_task(
    *,
    wftask: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
) -> TaskParameters:
    """
    Call a single task

    This assembles the runner arguments (input_paths, output_path, ...) and
    wftask arguments (i.e., arguments that are specific to the WorkflowTask,
    such as message or index in the dummy task), writes them to file, call the
    task executable command passing the arguments file as an input and
    assembles the output.

    **Note**: This function is directly submitted to a
    `concurrent.futures`-compatible executor, as in

        some_future = executor.submit(call_single_task, ...)

    If the executor then impersonates another user (as in the
    `FractalSlurmExecutor`), this function is run by that user.  For this
    reason, it should not write any file to workflow_dir, or it may yield
    permission errors.

    Args:
        wftask:
            The workflow task to be called. This includes task specific
            arguments via the wftask.args attribute.
        task_pars:
            The parameters required to run the task which are not specific to
            the task, e.g., I/O paths.
        workflow_dir:
            The server-side working directory for workflow execution.
        workflow_dir_user:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors). If `None`, it is set to be
            equal to `workflow_dir`.
    Returns:
        out_task_parameters:
            A TaskParameters in which the previous output becomes the input
            and where metadata is the metadata dictionary returned by the task
            being called.

    Raises:
        TaskExecutionError: If the wrapped task raises a task-related error.
                            This function is responsible of adding debugging
                            information to the TaskExecutionError, such as task
                            order and name.
        JobExecutionError: If the wrapped task raises a job-related error.
        RuntimeError: If the `workflow_dir` is falsy.
    """
    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    task_files = get_task_file_paths(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=wftask.order,
    )

    # write args file (by assembling task_pars and wftask.args)
    write_args_file(
        task_pars.dict(),
        wftask.args or {},
        path=task_files.args,
    )

    # assemble full command
    cmd = (
        f"{wftask.task.command} -j {task_files.args} "
        f"--metadata-out {task_files.metadiff}"
    )

    try:
        _call_command_wrapper(
            cmd, stdout=task_files.out, stderr=task_files.err
        )
    except TaskExecutionError as e:
        e.workflow_task_order = wftask.order
        e.workflow_task_id = wftask.id
        e.task_name = wftask.task.name
        raise e

    # NOTE:
    # This assumes that the new metadata is printed to stdout
    # and nothing else outputs to stdout
    with task_files.metadiff.open("r") as f_metadiff:
        diff_metadata = json.load(f_metadiff)
    updated_metadata = task_pars.metadata.copy()
    updated_metadata.update(diff_metadata)

    # Assemble a TaskParameter object
    HISTORY_LEGACY = f"{wftask.task.name}"
    try:
        updated_metadata["HISTORY_LEGACY"].append(HISTORY_LEGACY)
    except KeyError:
        updated_metadata["HISTORY_LEGACY"] = [HISTORY_LEGACY]

    # Update history
    wftask_dump = wftask.dict(exclude={"task"})
    wftask_dump["task"] = wftask.task.dict()
    new_history_item = dict(
        workflowtask=wftask_dump,
        status=WorkflowTaskStatusType.DONE,
        parallelization=None,
    )
    try:
        updated_metadata["history"].append(new_history_item)
    except KeyError:
        updated_metadata["history"] = [new_history_item]

    out_task_parameters = TaskParameters(
        input_paths=[task_pars.output_path],
        output_path=task_pars.output_path,
        metadata=updated_metadata,
    )

    return out_task_parameters


def call_single_parallel_task(
    component: str,
    *,
    wftask: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
) -> None:
    """
    Call a single instance of a parallel task

    Parallel tasks need to run in several instances across the parallelization
    parameters. This function is responsible of running each single one of
    those instances.

    Note:
        This function is directly submitted to a
        `concurrent.futures`-compatible executor, roughly as in

            some_future = executor.map(call_single_parallel_task, ...)

        If the executor then impersonates another user (as in the
        `FractalSlurmExecutor`), this function is run by that user.

    Args:
        component:
            The parallelization parameter.
        wftask:
            The task to execute.
        task_pars:
            The parameters to pass on to the task.
        workflow_dir:
            The server-side working directory for workflow execution.
        workflow_dir_user:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors).

    Raises:
        TaskExecutionError: If the wrapped task raises a task-related error.
                            This function is responsible of adding debugging
                            information to the TaskExecutionError, such as task
                            order and name.
        JobExecutionError: If the wrapped task raises a job-related error.
        RuntimeError: If the `workflow_dir` is falsy.
    """
    if not workflow_dir:
        raise RuntimeError
    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    task_files = get_task_file_paths(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=wftask.order,
        component=component,
    )

    # write args file (by assembling task_pars, wftask.args and component)
    write_args_file(
        task_pars.dict(),
        wftask.args or {},
        dict(component=component),
        path=task_files.args,
    )

    # assemble full command
    cmd = (
        f"{wftask.task.command} -j {task_files.args} "
        f"--metadata-out {task_files.metadiff}"
    )

    try:
        _call_command_wrapper(
            cmd, stdout=task_files.out, stderr=task_files.err
        )
    except TaskExecutionError as e:
        e.workflow_task_order = wftask.order
        e.workflow_task_id = wftask.id
        e.task_name = wftask.task.name
        raise e


def call_parallel_task(
    *,
    executor: Executor,
    wftask: WorkflowTask,
    task_pars_depend: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
) -> TaskParameters:
    """
    Collect results from the parallel instances of a parallel task

    Prepare and submit for execution all the single calls of a parallel task,
    and return a single TaskParameters instance to be passed on to the
    next task.

    Args:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        wftask:
            The parallel task to run.
        task_pars_depend:
            The task parameters to be passed on to the parallel task.
        workflow_dir:
            The server-side working directory for workflow execution.
        workflow_dir_user:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors).
        submit_setup_call:
            An optional function that computes configuration parameters for
            the executor.

    Returns:
        out_task_parameters:
            The output task parameters of the parallel task execution, ready to
            be passed on to the next task.
    """

    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    try:
        component_list = task_pars_depend.metadata[
            wftask.parallelization_level
        ]
    except KeyError:
        keys = list(task_pars_depend.metadata.keys())
        raise RuntimeError(
            "WorkflowTask parallelization_level "
            f"('{wftask.parallelization_level}') is missing "
            f"in metadata keys ({keys})."
        )

    # Backend-specific configuration
    try:
        extra_setup = submit_setup_call(
            wftask=wftask,
            task_pars=task_pars_depend,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
        )
    except Exception as e:
        tb = "".join(traceback.format_tb(e.__traceback__))
        raise RuntimeError(
            f"{type(e)} error in {submit_setup_call=}\n"
            f"Original traceback:\n{tb}"
        )

    # Preliminary steps
    partial_call_task = partial(
        call_single_parallel_task,
        wftask=wftask,
        task_pars=task_pars_depend,
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
    )

    # Submit tasks for execution. Note that `for _ in map_iter:
    # pass` explicitly calls the .result() method for each future, and
    # therefore is blocking until the task are complete.
    map_iter = executor.map(partial_call_task, component_list, **extra_setup)

    # Wait for execution of this chunk of tasks. Note: this is required *also*
    # because otherwise the shutdown of a FractalSlurmExecutor while running
    # map() may not work
    for _ in map_iter:
        pass  # noqa: 701

    # Assemble a TaskParameter object
    HISTORY_LEGACY = f"{wftask.task.name}: {component_list}"
    try:
        task_pars_depend.metadata["HISTORY_LEGACY"].append(HISTORY_LEGACY)
    except KeyError:
        task_pars_depend.metadata["HISTORY_LEGACY"] = [HISTORY_LEGACY]

    # Update history
    wftask_dump = wftask.dict(exclude={"task"})
    wftask_dump["task"] = wftask.task.dict()
    new_history_item = dict(
        workflowtask=wftask_dump,
        status="done",
        parallelization=dict(
            parallelization_level=wftask.parallelization_level,
            component_list=component_list,
        ),
    )
    try:
        task_pars_depend.metadata["history"].append(new_history_item)
    except KeyError:
        task_pars_depend.metadata["history"] = [new_history_item]

    out_task_parameters = TaskParameters(
        input_paths=[task_pars_depend.output_path],
        output_path=task_pars_depend.output_path,
        metadata=task_pars_depend.metadata,
    )

    return out_task_parameters


def execute_tasks(
    *,
    executor: Executor,
    task_list: list[WorkflowTask],
    task_pars: TaskParameters,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    logger_name: str,
) -> TaskParameters:
    """
    Submit a list of WorkflowTasks for execution

    **Note:** At the end of each task, write current metadata to `working_dir /
    METADATA_FILENAME`, so that they can be read as part of the [`get_job`
    endpoint](../../api/v1/job/#fractal_server.app.api.v1.job.get_job).

    Arguments:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        task_list:
            The list of wftasks to be run
        task_pars:
            The task parameters to be passed on to the first task of the list.
        workflow_dir:
            The server-side working directory for workflow execution.
        workflow_dir_user:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors). If `None`, it is set to be
            equal to `workflow_dir`.
        submit_setup_call:
            An optional function that computes configuration parameters for
            the executor.
        logger_name:
            Name of the logger

    Returns:
        current_task_pars:
            A TaskParameters object which constitutes the output of the last
            task in the list.
    """
    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    logger = get_logger(logger_name)

    current_task_pars = task_pars.copy()

    for this_wftask in task_list:
        logger.debug(
            f"SUBMIT {this_wftask.order}-th task "
            f'(name="{this_wftask.task.name}")'
        )
        if this_wftask.is_parallel:
            current_task_pars = call_parallel_task(
                executor=executor,
                wftask=this_wftask,
                task_pars_depend=current_task_pars,
                workflow_dir=workflow_dir,
                workflow_dir_user=workflow_dir_user,
                submit_setup_call=submit_setup_call,
            )
        else:
            # Call backend-specific submit_setup_call
            try:
                extra_setup = submit_setup_call(
                    wftask=this_wftask,
                    task_pars=current_task_pars,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                )
            except Exception as e:
                tb = "".join(traceback.format_tb(e.__traceback__))
                raise RuntimeError(
                    f"{type(e)} error in {submit_setup_call=}\n"
                    f"Original traceback:\n{tb}"
                )
            # NOTE: executor.submit(call_single_task, ...) is non-blocking,
            # i.e. the returned future may have `this_wftask_future.done() =
            # False`. We make it blocking right away, by calling `.result()`
            this_wftask_future = executor.submit(
                call_single_task,
                wftask=this_wftask,
                task_pars=current_task_pars,
                workflow_dir=workflow_dir,
                workflow_dir_user=workflow_dir_user,
                **extra_setup,
            )
            # Wait for the future result (blocking)
            current_task_pars = this_wftask_future.result()
        logger.debug(
            f"END    {this_wftask.order}-th task "
            f'(name="{this_wftask.task.name}")'
        )

        # Write most recent metadata to METADATA_FILENAME
        with open(workflow_dir / METADATA_FILENAME, "w") as f:
            json.dump(current_task_pars.metadata, f, indent=2)

    return current_task_pars
