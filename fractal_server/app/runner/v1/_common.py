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
from copy import deepcopy
from functools import partial
from pathlib import Path
from shlex import split as shlex_split
from typing import Any
from typing import Callable
from typing import Optional

from ....config import get_settings
from ....logger import get_logger
from ....syringe import Inject
from ...models.v1 import Task
from ...models.v1 import WorkflowTask
from ...schemas.v1 import WorkflowTaskStatusTypeV1
from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from .common import TaskParameters
from .common import write_args_file
from fractal_server.app.runner.filenames import HISTORY_FILENAME
from fractal_server.app.runner.filenames import METADATA_FILENAME
from fractal_server.app.runner.task_files import get_task_file_paths


def no_op_submit_setup_call(
    *,
    wftask: WorkflowTask,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
) -> dict:
    """
    Default (no-operation) interface of submit_setup_call.
    """
    return {}


def _task_needs_image_list(_task: Task) -> bool:
    """
    Whether a task requires `metadata["image"]` in its `args.json` file.

    For details see
    https://github.com/fractal-analytics-platform/fractal-server/issues/1237

    Args:
        _task: The task to be checked.
    """
    settings = Inject(get_settings)
    exception_task_names = settings.FRACTAL_RUNNER_TASKS_INCLUDE_IMAGE.split(
        ";"
    )
    if _task.name in exception_task_names:
        return True
    else:
        return False


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
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
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
    reason, it should not write any file to `workflow_dir_local`, or it may
    yield permission errors.

    Args:
        wftask:
            The workflow task to be called. This includes task specific
            arguments via the wftask.args attribute.
        task_pars:
            The parameters required to run the task which are not specific to
            the task, e.g., I/O paths.
        workflow_dir_local:
            The server-side working directory for workflow execution.
        workflow_dir_remote:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors). If `None`, it is set to be
            equal to `workflow_dir_remote`.
        logger_name:
            Name of the logger

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
    """

    logger = get_logger(logger_name)

    if not workflow_dir_remote:
        workflow_dir_remote = workflow_dir_local

    task_files = get_task_file_paths(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
    )

    # write args file (by assembling task_pars and wftask.args)
    write_args_file(
        task_pars.dict(exclude={"history"}),
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

    # This try/except block covers the case of a task that ran successfully but
    # did not write the expected metadiff file (ref fractal-server issue #854).
    try:
        with task_files.metadiff.open("r") as f_metadiff:
            diff_metadata = json.load(f_metadiff)
    except FileNotFoundError as e:
        logger.warning(
            f"Skip collection of updated metadata. Original error: {str(e)}"
        )
        diff_metadata = {}

    # Cover the case where the task wrote `null`, rather than a valid
    # dictionary (ref fractal-server issue #878).
    if diff_metadata is None:
        diff_metadata = {}

    # Prepare updated_metadata
    updated_metadata = task_pars.metadata.copy()
    updated_metadata.update(diff_metadata)
    # Prepare updated_history (note: the expected type for history items is
    # defined in `_DatasetHistoryItem`)
    wftask_dump = wftask.model_dump(exclude={"task"})
    wftask_dump["task"] = wftask.task.model_dump()
    new_history_item = dict(
        workflowtask=wftask_dump,
        status=WorkflowTaskStatusTypeV1.DONE,
        parallelization=None,
    )
    updated_history = task_pars.history.copy()
    updated_history.append(new_history_item)

    # Assemble a TaskParameter object
    out_task_parameters = TaskParameters(
        input_paths=[task_pars.output_path],
        output_path=task_pars.output_path,
        metadata=updated_metadata,
        history=updated_history,
    )

    return out_task_parameters


def call_single_parallel_task(
    component: str,
    *,
    wftask: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
) -> Any:
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
        workflow_dir_local:
            The server-side working directory for workflow execution.
        workflow_dir_remote:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors).

    Returns:
        The `json.load`-ed contents of the metadiff output file, or `None` if
            the file is missing.

    Raises:
        TaskExecutionError: If the wrapped task raises a task-related error.
                            This function is responsible of adding debugging
                            information to the TaskExecutionError, such as task
                            order and name.
        JobExecutionError: If the wrapped task raises a job-related error.
        RuntimeError: If the `workflow_dir_local` is falsy.
    """
    if not workflow_dir_local:
        raise RuntimeError
    if not workflow_dir_remote:
        workflow_dir_remote = workflow_dir_local

    task_files = get_task_file_paths(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
        component=component,
    )

    # write args file (by assembling task_pars, wftask.args and component)
    write_args_file(
        task_pars.dict(exclude={"history"}),
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

    # JSON-load metadiff file and return its contents (or None)
    try:
        with task_files.metadiff.open("r") as f:
            this_meta_update = json.load(f)
    except FileNotFoundError:
        this_meta_update = None

    return this_meta_update


def trim_TaskParameters(
    task_params: TaskParameters,
    _task: Task,
) -> TaskParameters:
    """
    Return a smaller copy of a TaskParameter object.

    Remove metadata["image"] key/value pair - see issues 1237 and 1242.
    (https://github.com/fractal-analytics-platform/fractal-server/issues/1237)
    This applies only to parallel tasks with names different from the ones
    defined in `_task_needs_image_list`.
    """
    task_params_slim = deepcopy(task_params)
    if not _task_needs_image_list(_task) and _task.is_parallel:
        if "image" in task_params_slim.metadata.keys():
            task_params_slim.metadata.pop("image")
        task_params_slim.history = []
    return task_params_slim


def call_parallel_task(
    *,
    executor: Executor,
    wftask: WorkflowTask,
    task_pars_depend: TaskParameters,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    logger_name: Optional[str] = None,
) -> TaskParameters:
    """
    Collect results from the parallel instances of a parallel task

    Prepare and submit for execution all the single calls of a parallel task,
    and return a single TaskParameters instance to be passed on to the
    next task.

    **NOTE**: this function is executed by the same user that runs
    `fractal-server`, and therefore may not have access to some of user's
    files.

    Args:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        wftask:
            The parallel task to run.
        task_pars_depend:
            The task parameters to be passed on to the parallel task.
        workflow_dir_local:
            The server-side working directory for workflow execution.
        workflow_dir_remote:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors).
        submit_setup_call:
            An optional function that computes configuration parameters for
            the executor.
        logger_name:
            Name of the logger

    Returns:
        out_task_parameters:
            The output task parameters of the parallel task execution, ready to
            be passed on to the next task.
    """
    logger = get_logger(logger_name)

    if not workflow_dir_remote:
        workflow_dir_remote = workflow_dir_local

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
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
        )
    except Exception as e:
        tb = "".join(traceback.format_tb(e.__traceback__))
        raise RuntimeError(
            f"{type(e)} error in {submit_setup_call=}\n"
            f"Original traceback:\n{tb}"
        )

    # Preliminary steps
    actual_task_pars_depend = trim_TaskParameters(
        task_pars_depend, wftask.task
    )

    partial_call_task = partial(
        call_single_parallel_task,
        wftask=wftask,
        task_pars=actual_task_pars_depend,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
    )

    # Submit tasks for execution. Note that `for _ in map_iter:
    # pass` explicitly calls the .result() method for each future, and
    # therefore is blocking until the task are complete.
    map_iter = executor.map(partial_call_task, component_list, **extra_setup)

    # Wait for execution of parallel tasks, and aggregate updated metadata (ref
    # https://github.com/fractal-analytics-platform/fractal-server/issues/802).
    # NOTE: Even if we remove the need of aggregating metadata, we must keep
    # the iteration over `map_iter` (e.g. as in `for _ in map_iter: pass`), to
    # make this call blocking. This is required *also* because otherwise the
    # shutdown of a FractalSlurmExecutor while running map() may not work
    aggregated_metadata_update: dict[str, Any] = {}
    for this_meta_update in map_iter:
        # Cover the case where the task wrote `null`, rather than a
        # valid dictionary (ref fractal-server issue #878), or where the
        # metadiff file was missing.
        if this_meta_update is None:
            this_meta_update = {}
        # Include this_meta_update into aggregated_metadata_update
        for key, val in this_meta_update.items():
            aggregated_metadata_update.setdefault(key, []).append(val)
    if aggregated_metadata_update:
        logger.warning(
            "Aggregating parallel-taks updated metadata (with keys "
            f"{list(aggregated_metadata_update.keys())}).\n"
            "This feature is experimental and it may change in "
            "future releases."
        )

    # Prepare updated_metadata
    updated_metadata = task_pars_depend.metadata.copy()
    updated_metadata.update(aggregated_metadata_update)

    # Prepare updated_history (note: the expected type for history items is
    # defined in `_DatasetHistoryItem`)
    wftask_dump = wftask.model_dump(exclude={"task"})
    wftask_dump["task"] = wftask.task.model_dump()
    new_history_item = dict(
        workflowtask=wftask_dump,
        status=WorkflowTaskStatusTypeV1.DONE,
        parallelization=dict(
            parallelization_level=wftask.parallelization_level,
            component_list=component_list,
        ),
    )
    updated_history = task_pars_depend.history.copy()
    updated_history.append(new_history_item)

    # Assemble a TaskParameter object
    out_task_parameters = TaskParameters(
        input_paths=[task_pars_depend.output_path],
        output_path=task_pars_depend.output_path,
        metadata=updated_metadata,
        history=updated_history,
    )

    return out_task_parameters


def execute_tasks(
    *,
    executor: Executor,
    task_list: list[WorkflowTask],
    task_pars: TaskParameters,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    logger_name: str,
) -> TaskParameters:
    """
    Submit a list of WorkflowTasks for execution

    **Note:** At the end of each task, write current metadata to
    `workflow_dir_local / METADATA_FILENAME`, so that they can be read as part
    of the [`get_job`
    endpoint](../../api/v1/job/#fractal_server.app.routes.api.v1.job.get_job).

    Arguments:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        task_list:
            The list of wftasks to be run
        task_pars:
            The task parameters to be passed on to the first task of the list.
        workflow_dir_local:
            The server-side working directory for workflow execution.
        workflow_dir_remote:
            The user-side working directory for workflow execution (only
            relevant for multi-user executors). If `None`, it is set to be
            equal to `workflow_dir_local`.
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
    if not workflow_dir_remote:
        workflow_dir_remote = workflow_dir_local

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
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                submit_setup_call=submit_setup_call,
                logger_name=logger_name,
            )
        else:
            # Call backend-specific submit_setup_call
            try:
                extra_setup = submit_setup_call(
                    wftask=this_wftask,
                    workflow_dir_local=workflow_dir_local,
                    workflow_dir_remote=workflow_dir_remote,
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
            # NOTE: do not use trim_TaskParameters for non-parallel tasks,
            # since the `task_pars` argument in `call_single_task` is also used
            # as a basis for new `metadata`.
            this_wftask_future = executor.submit(
                call_single_task,
                wftask=this_wftask,
                task_pars=current_task_pars,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                logger_name=logger_name,
                **extra_setup,
            )
            # Wait for the future result (blocking)
            current_task_pars = this_wftask_future.result()
        logger.debug(
            f"END    {this_wftask.order}-th task "
            f'(name="{this_wftask.task.name}")'
        )

        # Write most recent metadata to METADATA_FILENAME
        with open(workflow_dir_local / METADATA_FILENAME, "w") as f:
            json.dump(current_task_pars.metadata, f, indent=2)

        # Write most recent metadata to HISTORY_FILENAME
        with open(workflow_dir_local / HISTORY_FILENAME, "w") as f:
            json.dump(current_task_pars.history, f, indent=2)

    return current_task_pars
