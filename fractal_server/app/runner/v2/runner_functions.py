import functools
import logging
import traceback
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

from pydantic import ValidationError

from ..exceptions import JobExecutionError
from .deduplicate_list import deduplicate_list
from .merge_outputs import merge_outputs
from .runner_functions_low_level import run_single_task
from .task_interface import InitTaskOutput
from .task_interface import TaskOutput
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.components import _index_to_component
from fractal_server.app.runner.v2._local.executor import BaseRunner


__all__ = [
    "run_v2_task_non_parallel",
    "run_v2_task_parallel",
    "run_v2_task_compound",
]

MAX_PARALLELIZATION_LIST_SIZE = 20_000


def _cast_and_validate_TaskOutput(
    task_output: dict[str, Any]
) -> Optional[TaskOutput]:
    try:
        validated_task_output = TaskOutput(**task_output)
        return validated_task_output
    except ValidationError as e:
        raise JobExecutionError(
            "Validation of task output failed.\n"
            f"Original error: {str(e)}\n"
            f"Original data: {task_output}."
        )


def _cast_and_validate_InitTaskOutput(
    init_task_output: dict[str, Any],
) -> Optional[InitTaskOutput]:
    try:
        validated_init_task_output = InitTaskOutput(**init_task_output)
        return validated_init_task_output
    except ValidationError as e:
        raise JobExecutionError(
            "Validation of init-task output failed.\n"
            f"Original error: {str(e)}\n"
            f"Original data: {init_task_output}."
        )


def no_op_submit_setup_call(
    *,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    which_type: Literal["non_parallel", "parallel"],
) -> dict:
    """
    Default (no-operation) interface of submit_setup_call in V2.
    """
    return {}


# Backend-specific configuration
def _get_executor_options(
    *,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    submit_setup_call: Callable,
    which_type: Literal["non_parallel", "parallel"],
) -> dict:
    try:
        options = submit_setup_call(
            wftask=wftask,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            which_type=which_type,
        )
    except Exception as e:
        tb = "".join(traceback.format_tb(e.__traceback__))
        raise RuntimeError(
            f"{type(e)} error in {submit_setup_call=}\n"
            f"Original traceback:\n{tb}"
        )
    return options


def _check_parallelization_list_size(my_list):
    if len(my_list) > MAX_PARALLELIZATION_LIST_SIZE:
        raise JobExecutionError(
            "Too many parallelization items.\n"
            f"   {len(my_list)}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )


def run_v2_task_non_parallel(
    *,
    images: list[dict[str, Any]],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    executor: BaseRunner,
    submit_setup_call: Callable = no_op_submit_setup_call,
    history_item_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    """
    This runs server-side (see `executor` argument)
    """

    if workflow_dir_remote is None:
        workflow_dir_remote = workflow_dir_local
        logging.warning(
            "In `run_single_task`, workflow_dir_remote=None. Is this right?"
        )
        workflow_dir_remote = workflow_dir_local

    executor_options = _get_executor_options(
        wftask=wftask,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        submit_setup_call=submit_setup_call,
        which_type="non_parallel",
    )

    function_kwargs = dict(
        zarr_urls=[image["zarr_url"] for image in images],
        zarr_dir=zarr_dir,
        **(wftask.args_non_parallel or {}),
    )
    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
        ),
        parameters=function_kwargs,
        history_item_id=history_item_id,
        **executor_options,
    )

    num_tasks = 1
    if exception is None:
        if result is None:
            return (TaskOutput(), num_tasks, {})
        else:
            return (_cast_and_validate_TaskOutput(result), num_tasks, {})
    else:
        return (TaskOutput(), num_tasks, {0: exception})


def run_v2_task_parallel(
    *,
    images: list[dict[str, Any]],
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    history_item_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:

    if len(images) == 0:
        return (TaskOutput(), 0, {})

    _check_parallelization_list_size(images)

    executor_options = _get_executor_options(
        wftask=wftask,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        submit_setup_call=submit_setup_call,
        which_type="parallel",
    )

    list_function_kwargs = []
    for ind, image in enumerate(images):
        list_function_kwargs.append(
            dict(
                zarr_url=image["zarr_url"],
                **(wftask.args_parallel or {}),
            ),
        )
        list_function_kwargs[-1][_COMPONENT_KEY_] = _index_to_component(ind)

    results, exceptions = executor.multisubmit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
        ),
        list_parameters=list_function_kwargs,
        history_item_id=history_item_id,
        **executor_options,
    )

    outputs = []
    for ind in range(len(list_function_kwargs)):
        if ind in results.keys():
            result = results[ind]
            if result is None:
                output = TaskOutput()
            else:
                output = _cast_and_validate_TaskOutput(result)
            outputs.append(output)
        elif ind in exceptions.keys():
            print(f"Bad: {exceptions[ind]}")
        else:
            print("VERY BAD - should have not reached this point")

    num_tasks = len(images)
    merged_output = merge_outputs(outputs)
    return (merged_output, num_tasks, exceptions)


def run_v2_task_compound(
    *,
    images: list[dict[str, Any]],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    history_item_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:

    executor_options_init = _get_executor_options(
        wftask=wftask,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        submit_setup_call=submit_setup_call,
        which_type="non_parallel",
    )
    executor_options_compute = _get_executor_options(
        wftask=wftask,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        submit_setup_call=submit_setup_call,
        which_type="parallel",
    )

    # 3/A: non-parallel init task
    function_kwargs = dict(
        zarr_urls=[image["zarr_url"] for image in images],
        zarr_dir=zarr_dir,
        **(wftask.args_non_parallel or {}),
    )
    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
        ),
        parameters=function_kwargs,
        history_item_id=history_item_id,
        init_of_compound_task=True,
        **executor_options_init,
    )

    num_tasks = 1
    if exception is None:
        if result is None:
            init_task_output = InitTaskOutput()
        else:
            init_task_output = _cast_and_validate_InitTaskOutput(result)
    else:
        return (TaskOutput(), num_tasks, {0: exception})

    parallelization_list = init_task_output.parallelization_list
    parallelization_list = deduplicate_list(parallelization_list)

    num_tasks = 1 + len(parallelization_list)

    # 3/B: parallel part of a compound task
    _check_parallelization_list_size(parallelization_list)

    if len(parallelization_list) == 0:
        return (TaskOutput(), 0, {})

    list_function_kwargs = []
    for ind, parallelization_item in enumerate(parallelization_list):
        list_function_kwargs.append(
            dict(
                zarr_url=parallelization_item.zarr_url,
                init_args=parallelization_item.init_args,
                **(wftask.args_parallel or {}),
            ),
        )
        list_function_kwargs[-1][_COMPONENT_KEY_] = _index_to_component(ind)

    results, exceptions = executor.multisubmit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
        ),
        list_parameters=list_function_kwargs,
        history_item_id=history_item_id,
        compute_of_compound_task=True,
        **executor_options_compute,
    )

    outputs = []
    for ind in range(len(list_function_kwargs)):
        if ind in results.keys():
            result = results[ind]
            if result is None:
                output = TaskOutput()
            else:
                output = _cast_and_validate_TaskOutput(result)
            outputs.append(output)
        elif ind in exceptions.keys():
            print(f"Bad: {exceptions[ind]}")

    merged_output = merge_outputs(outputs)
    return (merged_output, num_tasks, exceptions)
