import json
import logging
import subprocess  # nosec
from concurrent.futures import Executor
from concurrent.futures import Future
from functools import partial
from pathlib import Path
from shlex import split as shlex_split
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from ..models import WorkflowTask
from .common import TaskParameterEncoder
from .common import TaskParameters
from .common import write_args_file


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


class TaskExecutionError(RuntimeError):
    pass


def _call_command_wrapper(cmd: str, stdout: Path, stderr: Path) -> None:
    """
    Call command and return stdout, stderr, retcode
    """
    fp_stdout = stdout.open("w")
    fp_stderr = stderr.open("w")
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

    This assemble the runner (input_paths, output_path, ...) and task
    arguments (arguments that are specific to the task, such as message or
    index in the dummy task), writes them to file, call the task executable
    command passing the arguments file as an input and assembles the output

    Attributes
    ----------
    task (WorkflowTask):
        the workflow task to be called. This includes task specific arguments
        via the task.task.arguments attribute.
    task_pars (TaskParameters):
        the parameters required to run the task which are not specific to the
        task, e.g., I/O paths.
    workflow_dir (Path):
        the directory in which the execution takes place, and where all
        artifacts are written.

    Return
    ------
    out_task_parameters (TaskParameters):
        a TaskParameters in which the previous output becomes the input and
        where metadata is the metadata dictionary returned by the task being
        called.
    """
    if not workflow_dir:
        raise RuntimeError

    logger = logging.getLogger(task_pars.logger_name)

    stdout_file = workflow_dir / f"{task.order}.out"
    stderr_file = workflow_dir / f"{task.order}.err"
    metadata_diff_file = workflow_dir / f"{task.order}.metadiff.json"

    # assemble full args
    args_dict = task.assemble_args(extra=task_pars.dict())

    # write args file
    args_file_path = workflow_dir / f"{task.order}.args.json"
    write_args_file(args=args_dict, path=args_file_path)

    # assemble full command
    cmd = (
        f"{task.task.command} -j {args_file_path} "
        f"--metadata-out {metadata_diff_file}"
    )

    logger.debug(f"executing task {task.order=}")
    _call_command_wrapper(cmd, stdout=stdout_file, stderr=stderr_file)

    # NOTE:
    # This assumes that the new metadata is printed to stdout
    # and nothing else outputs to stdout
    with metadata_diff_file.open("r") as f_metadiff:
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
    return out_task_parameters


def call_single_parallel_task(
    component: str,
    *,
    task: WorkflowTask,
    task_pars: TaskParameters,
    workflow_dir: Path = None,
) -> None:
    if not workflow_dir:
        raise RuntimeError
    logger = logging.getLogger(task_pars.logger_name)

    component_safe = sanitize_component(component)
    prefix = f"{task.order}_par_{component_safe}"
    stdout_file = workflow_dir / f"{prefix}.out"
    stderr_file = workflow_dir / f"{prefix}.err"
    metadata_diff_file = workflow_dir / f"{prefix}.metadiff.json"

    logger.debug(f"calling task {task.order=} on {component=}")
    # FIXME refactor with `write_args_file` and `task.assemble_args`
    # assemble full args
    args_dict = task_pars.dict()
    args_dict.update(task.arguments)
    args_dict["component"] = component

    # write args file
    args_file_path = workflow_dir / f"{prefix}.args.json"
    with open(args_file_path, "w") as f:
        json.dump(args_dict, f, cls=TaskParameterEncoder, indent=4)
    # FIXME: UP TO HERE

    # assemble full command
    cmd = (
        f"{task.task.command} -j {args_file_path} "
        f"--metadata-out {metadata_diff_file}"
    )

    logger.debug(f"executing task {task.order=}")
    _call_command_wrapper(cmd, stdout=stdout_file, stderr=stderr_file)


def call_parallel_task(
    *,
    executor: Executor,
    task: WorkflowTask,
    task_pars_depend_future: Future,  # py3.9 Future[TaskParameters],
    workflow_dir: Path,
    extra_submit_dict: Optional[Dict[str, Any]] = None,
) -> Future:  # py3.9 Future[TaskParameters]:
    """
    AKA collect results
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
    if extra_submit_dict is None:
        extra_submit_dict = {}
    map_iter = executor.map(
        partial_call_task, component_list, **extra_submit_dict
    )
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

    this_future: Future = Future()
    out_task_parameters = TaskParameters(
        input_paths=[task_pars_depend.output_path],
        output_path=task_pars_depend.output_path,
        metadata=task_pars_depend.metadata,
        logger_name=task_pars_depend.logger_name,
    )
    this_future.set_result(out_task_parameters)
    return this_future


def recursive_task_submission(
    *,
    executor: Executor,
    task_list: List[WorkflowTask],
    task_pars: TaskParameters,
    workflow_dir: Path,
    submit_setup_call: Optional[
        Callable[[WorkflowTask], Dict[str, Any]]
    ] = lambda task: {},
) -> Future:
    """
    Recursively submit a list of task

    Each following task depends on the future.result() of the previous one,
    thus assuring the dependency chain.

    Induction process
    -----------------
    0: return a future which results in the task parameters necessary for the
       first task of the list

    n -> n+1: use output resulting from step `n` as task parameters to submit
       task `n+1`

    Return
    ------
    this_future (Future[TaskParameters]):
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

    extra_setup = submit_setup_call(this_task)

    if this_task.is_parallel:
        this_future = call_parallel_task(
            executor=executor,
            task=this_task,
            task_pars_depend_future=task_pars_depend_future,
            workflow_dir=workflow_dir,
        )
    else:
        this_future = executor.submit(
            call_single_task,
            task=this_task,
            task_pars=task_pars_depend_future.result(),
            workflow_dir=workflow_dir,
            **extra_setup,
        )
    return this_future
