import functools
from concurrent.futures import Executor
from pathlib import Path
from typing import Optional

from ....images import SingleImage
from ._v1_task_compatibility import _convert_v2_args_into_v1
from .deduplicate_list import deduplicate_list
from .merge_outputs import merge_outputs
from .runner_functions_low_level import _run_single_task
from .task_interface import InitArgsModel
from .task_interface import InitTaskOutput
from .task_interface import TaskOutput
from fractal_server.app.models.v1 import Task as TaskV1
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.v2.components import _COMPONENT_KEY_
from fractal_server.app.runner.v2.components import _index_to_component


__all__ = [
    "run_v2_task_non_parallel",
    "run_v2_task_parallel",
    "run_v2_task_compound",
    "run_v1_task_parallel",
]

MAX_PARALLELIZATION_LIST_SIZE = 200


def run_v2_task_non_parallel(
    *,
    images: list[SingleImage],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
    executor: Executor,
) -> TaskOutput:
    """
    This runs server-side (see `executor` argument)
    """

    if not workflow_dir_user:
        workflow_dir_user = workflow_dir

    function_kwargs = dict(
        paths=[image.path for image in images],
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )
    future = executor.submit(
        functools.partial(
            _run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
        ),
        function_kwargs,
    )
    output = future.result()
    # FIXME V2: handle validation errors
    if output is None:
        return TaskOutput()
    else:
        validated_output = TaskOutput(**output)
        return validated_output


def run_v2_task_parallel(
    *,
    images: list[SingleImage],
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: Executor,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
) -> TaskOutput:
    if len(images) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(images)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    list_function_kwargs = []
    for ind, image in enumerate(images):
        list_function_kwargs.append(
            dict(
                path=image.path,
                **wftask.args_parallel,
            ),
        )
        list_function_kwargs[-1][_COMPONENT_KEY_] = _index_to_component(ind)

    results_iterator = executor.map(
        functools.partial(
            _run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
        ),
        list_function_kwargs,
    )
    # Explicitly iterate over the whole list, so that all futures are waited
    outputs = list(results_iterator)

    from devtools import debug

    debug("PARALLEL_TASK", outputs)
    # Validate all non-None outputs
    for ind, output in enumerate(outputs):
        if output is None:
            outputs[ind] = TaskOutput()
        else:
            # FIXME: improve handling of validation errors
            validated_output = TaskOutput(**output)
            outputs[ind] = validated_output

    merged_output = merge_outputs(outputs)
    return merged_output


def run_v2_task_compound(
    *,
    images: list[SingleImage],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: Executor,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
) -> TaskOutput:
    # 3/A: non-parallel init task
    function_kwargs = dict(
        paths=[image.path for image in images],
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )
    future = executor.submit(
        functools.partial(
            _run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
        ),
        function_kwargs,
    )
    output = future.result()
    from devtools import debug

    debug(output)
    if output is None:
        init_task_output = InitTaskOutput()
    else:
        init_task_output = InitTaskOutput(**output)
    parallelization_list = init_task_output.parallelization_list
    parallelization_list = deduplicate_list(
        parallelization_list, PydanticModel=InitArgsModel
    )

    # 3/B: parallel part of a compound task
    list_function_kwargs = []
    for ind, parallelization_item in enumerate(parallelization_list):
        list_function_kwargs.append(
            dict(
                path=parallelization_item.path,
                init_args=parallelization_item.init_args,
                **wftask.args_parallel,
            ),
        )
        list_function_kwargs[-1][_COMPONENT_KEY_] = _index_to_component(ind)

    results_iterator = executor.map(
        functools.partial(
            _run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
        ),
        list_function_kwargs,
    )
    # Explicitly iterate over the whole list, so that all futures are waited
    outputs = list(results_iterator)

    # Validate all non-None outputs
    for ind, output in enumerate(outputs):
        if output is None:
            outputs[ind] = TaskOutput()
        else:
            # FIXME: improve handling of validation errors
            validated_output = TaskOutput(**output)
            outputs[ind] = validated_output

    merged_output = merge_outputs(outputs)
    return merged_output


def run_v1_task_parallel(
    *,
    images: list[SingleImage],
    task_legacy: TaskV1,
    wftask: WorkflowTaskV2,
    executor: Executor,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
) -> TaskOutput:

    if len(images) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(images)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    list_function_kwargs = []
    for ind, image in enumerate(images):
        list_function_kwargs.append(
            _convert_v2_args_into_v1(
                dict(
                    path=image.path,
                    **wftask.args_parallel,
                )
            ),
        )
        list_function_kwargs[-1][_COMPONENT_KEY_] = _index_to_component(ind)

    results_iterator = executor.map(
        functools.partial(
            _run_single_task,
            wftask=wftask,
            command=task_legacy.command,
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
            is_task_v1=True,
        ),
        list_function_kwargs,
    )
    # Explicitly iterate over the whole list, so that all futures are waited
    list(results_iterator)

    # Ignore any output metadata for V1 tasks, and return an empty object
    out = TaskOutput()
    return out
