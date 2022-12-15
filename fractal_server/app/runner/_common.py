"""
Common utilities and routines for runner backends (private API)

This module includes utilities and routines that are of use to implement
runner backends and that should not be exposed outside of the runner
subsystem.
"""
import json
import logging
import subprocess  # nosec
from concurrent.futures import Executor
from concurrent.futures import Future
from functools import lru_cache
from functools import partial
from pathlib import Path
from shlex import split as shlex_split
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from ...utils import file_opener
from ..models import WorkflowTask
from .common import TaskExecutionError
from .common import TaskParameters
from .common import write_args_file


METADATA_FILENAME = "metadata.json"


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


class WorkflowFiles:
    """
    Group all file paths pertaining to a workflow
    """

    workflow_dir: Path
    task_order: Optional[int] = None
    component: Optional[str] = None

    prefix: str
    file_prefix: str
    args: Path
    out: Path
    err: Path
    metadiff: Path

    def __init__(
        self,
        workflow_dir: Path,
        task_order: Optional[int] = None,
        component: Optional[str] = None,
    ):
        self.workflow_dir = workflow_dir
        self.task_order = task_order
        self.component = component

        if self.component:
            component_safe = f"_par_{sanitize_component(self.component)}"
        else:
            component_safe = ""

        if self.task_order is not None:
            order = str(self.task_order)
        else:
            order = "task"
        self.prefix = f"{order}{component_safe}"
        self.args = self.workflow_dir / f"{self.prefix}.args.json"
        self.out = self.workflow_dir / f"{self.prefix}.out"
        self.err = self.workflow_dir / f"{self.prefix}.err"
        self.metadiff = self.workflow_dir / f"{self.prefix}.metadiff.json"
        self.file_prefix = str(self.task_order)


@lru_cache()
def get_workflow_file_paths(
    workflow_dir: Path,
    task_order: Optional[int] = None,
    component: Optional[str] = None,
) -> WorkflowFiles:
    """
    Return the corrisponding WorkflowFiles object

    This function is mainly used as a cache to avoid instantiating needless
    objects.
    """
    return WorkflowFiles(
        workflow_dir=workflow_dir, task_order=task_order, component=component
    )


def _call_command_wrapper(cmd: str, stdout: Path, stderr: Path) -> None:
    """
    Call command and return stdout, stderr, retcode
    """
    fp_stdout = open(stdout, "w", opener=file_opener)
    fp_stderr = open(stderr, "w", opener=file_opener)
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
    if result.returncode != 0:
        with stderr.open("r") as fp_stderr:
            err = fp_stderr.read()
        raise TaskExecutionError(err)


def call_single_task(
    *,
    task: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path = None,
) -> TaskParameters:
    """
    Call a single task

    This assembles the runner arguments (input_paths, output_path, ...) and
    task arguments (i.e., arguments that are specific to the task, such as
    message or index in the dummy task), writes them to file, call the task
    executable command passing the arguments file as an input and assembles
    the output.

    Args:
        task:
            the workflow task to be called. This includes task specific
            arguments via the task.task.arguments attribute.
        task_pars:
            the parameters required to run the task which are not specific to
            the task, e.g., I/O paths.
        workflow_dir:
            the directory in which the execution takes place, and where all
            artifacts are written.

    Returns:
        out_task_parameters (TaskParameters):
            a TaskParameters in which the previous output becomes the input
            and where metadata is the metadata dictionary returned by the task
            being called.

    Raises:
        TaskExecutionError: If the wrapped task raises an error. This function
            is responsible of adding debugging information to the
            TaskExecutionError, such as task order and name.
        RuntimeError: If the `workflow_dir` is falsy.
    """
    if not workflow_dir:
        raise RuntimeError

    logger = logging.getLogger(task_pars.logger_name)

    workflow_files = get_workflow_file_paths(
        workflow_dir=workflow_dir, task_order=task.order
    )

    # assemble full args
    args_dict = task.assemble_args(extra=task_pars.dict())

    # write args file
    write_args_file(args_dict, path=workflow_files.args)

    # assemble full command
    cmd = (
        f"{task.task.command} -j {workflow_files.args} "
        f"--metadata-out {workflow_files.metadiff}"
    )

    logger.debug(f"executing task {task.order=}")

    try:
        _call_command_wrapper(
            cmd, stdout=workflow_files.out, stderr=workflow_files.err
        )
    except TaskExecutionError as e:
        e.workflow_task_order = task.order
        e.workflow_task_id = task.id
        e.task_name = task.task.name
        raise e

    # NOTE:
    # This assumes that the new metadata is printed to stdout
    # and nothing else outputs to stdout
    with workflow_files.metadiff.open("r") as f_metadiff:
        diff_metadata = json.load(f_metadiff)
    updated_metadata = task_pars.metadata.copy()
    updated_metadata.update(diff_metadata)

    # Assemble a Future[TaskParameter]
    history = f"{task.task.name}"
    try:
        updated_metadata["history"].append(history)
    except KeyError:
        updated_metadata["history"] = [history]

    out_task_parameters = TaskParameters(
        input_paths=[task_pars.output_path],
        output_path=task_pars.output_path,
        metadata=updated_metadata,
        logger_name=task_pars.logger_name,
    )
    with open(workflow_dir / METADATA_FILENAME, "w", opener=file_opener) as f:
        json.dump(updated_metadata, f, indent=2)
    return out_task_parameters


def call_single_parallel_task(
    component: str,
    *,
    task: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path = None,
) -> None:
    """
    Call a single instance of a parallel task

    Parallel tasks need to run in several instances across the parallelisation
    parameters. This function is responsible of running each single one of
    those instances.

    Args:
        component:
            The parallelisation parameter.
        task:
            The task to execute.
        task_pars:
            The parameters to pass on to the task.
        workflow_dir:
            The workflow working directory.

    Raises:
        TaskExecutionError: If the wrapped task raises an error. This function
            is responsible of adding debugging information to the
            TaskExecutionError, such as task order and name.
        RuntimeError: If the `workflow_dir` is falsy.
    """
    if not workflow_dir:
        raise RuntimeError
    logger = logging.getLogger(task_pars.logger_name)

    workflow_files = get_workflow_file_paths(
        workflow_dir=workflow_dir, task_order=task.order, component=component
    )

    logger.debug(f"calling task {task.order=} on {component=}")
    # assemble full args
    write_args_file(
        task_pars.dict(),
        task.arguments,
        dict(component=component),
        path=workflow_files.args,
    )

    # assemble full command
    cmd = (
        f"{task.task.command} -j {workflow_files.args} "
        f"--metadata-out {workflow_files.metadiff}"
    )

    logger.debug(f"executing task {task.order=}")

    try:
        _call_command_wrapper(
            cmd, stdout=workflow_files.out, stderr=workflow_files.err
        )
    except TaskExecutionError as e:
        e.workflow_task_order = task.order
        e.workflow_task_id = task.id
        e.task_name = task.task.name
        raise e


def call_parallel_task(
    *,
    executor: Executor,
    task: WorkflowTask,
    task_pars_depend_future: Future,  # py3.9 Future[TaskParameters],
    workflow_dir: Path,
    submit_setup_call: Callable[
        [WorkflowTask, TaskParameters, Path], Dict[str, Any]
    ] = lambda task, task_pars, workflow_dir: {},
) -> Future:  # py3.9 Future[TaskParameters]:
    """
    Join results from the parallel instances of a parallel task

    AKA Collect results. This function merges all the results of single calls
    of a parallel task and return a single future with the TaskParameters to
    be passed on to the next task.

    Args:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        task:
            The parallel task to run.
        task_pars_depend_future:
            A future that will resolve in the task parameters to be passed on
            to the parallel task.
        workflow_dir:
            The workflow working directory.
        submit_setup_call:
            An optional function that computes configuration parameters for
            the executor.

    Returns:
        out_future:
            A future that resolves in the output task parameters of the
            parallel task execution, ready to be passed on to the next task.
    """
    task_pars_depend = task_pars_depend_future.result()
    component_list = task_pars_depend.metadata[task.parallelization_level]

    # Submit all tasks (one per component)
    partial_call_task = partial(
        call_single_parallel_task,
        task=task,
        task_pars=task_pars_depend,
        workflow_dir=workflow_dir,
    )

    extra_setup = submit_setup_call(task, task_pars_depend, workflow_dir)

    map_iter = executor.map(partial_call_task, component_list, **extra_setup)
    # Wait for execution of all parallel (this explicitly calls .result()
    # on each parallel task)
    for _ in map_iter:
        pass  # noqa: 701

    # Assemble a Future[TaskParameter]
    history = f"{task.task.name}: {component_list}"
    try:
        task_pars_depend.metadata["history"].append(history)
    except KeyError:
        task_pars_depend.metadata["history"] = [history]

    out_task_parameters = TaskParameters(
        input_paths=[task_pars_depend.output_path],
        output_path=task_pars_depend.output_path,
        metadata=task_pars_depend.metadata,
        logger_name=task_pars_depend.logger_name,
    )

    with open(workflow_dir / METADATA_FILENAME, "w", opener=file_opener) as f:
        json.dump(task_pars_depend.metadata, f, indent=2)
    out_future: Future = Future()
    out_future.set_result(out_task_parameters)
    return out_future


def recursive_task_submission(
    *,
    executor: Executor,
    task_list: List[WorkflowTask],
    task_pars: TaskParameters,
    workflow_dir: Path,
    submit_setup_call: Callable[
        [WorkflowTask, TaskParameters, Path], Dict[str, Any]
    ] = lambda task, task_pars, workflow_dir: {},
) -> Future:
    """
    Recursively submit a list of tasks

    This recursive function schedules a workflow task list in the correct
    order, making sure to resolve dependency before proceeding to the next
    task: each following task depends on the future.result() of the previous
    one, thus assuring the dependency chain.

    Induction process:

    `0`: return a future which resolves in the task parameters necessary for
        the first task of the list.

    `n => n+1`: use output resulting from step `n` as input for the first task
        in the list, i.e., the `n+1`st task.

    Args:
        executor:
            The `concurrent.futures.Executor`-compatible executor that will
            run the task.
        task_list:
            The list of tasks to be run
        task_pars:
            The task parameters to be passed on to the first task of the list.
        workflow_dir:
            The workflow working directory.
        submit_setup_call:
            An optional function that computes configuration parameters for
            the executor.

    Returns:
        this_task_future:
            a future that results to the task parameters which constitute the
            input of the following task in the list.
    """
    try:
        *dependencies, this_task = task_list
    except ValueError:
        # step 0: return future containing original task_pars
        pseudo_future: Future = Future()
        pseudo_future.set_result(task_pars)
        return pseudo_future

    logger = logging.getLogger(task_pars.logger_name)

    # step n => step n+1
    logger.debug(f"submitting task {this_task.order=}")

    task_pars_depend_future = recursive_task_submission(
        executor=executor,
        task_list=dependencies,
        task_pars=task_pars,
        workflow_dir=workflow_dir,
        submit_setup_call=submit_setup_call,
    )

    if this_task.is_parallel:
        this_task_future = call_parallel_task(
            executor=executor,
            task=this_task,
            task_pars_depend_future=task_pars_depend_future,
            workflow_dir=workflow_dir,
            submit_setup_call=submit_setup_call,
        )
    else:
        extra_setup = submit_setup_call(this_task, task_pars, workflow_dir)
        this_task_future = executor.submit(
            call_single_task,
            task=this_task,
            task_pars=task_pars_depend_future.result(),
            workflow_dir=workflow_dir,
            **extra_setup,
        )
    return this_task_future
