import functools
from concurrent.futures import Executor

from ....images import SingleImage
from ._v1_task_compatibility import _convert_v2_args_into_v1
from .deduplicate_list import deduplicate_list
from .merge_outputs import merge_outputs
from .models import Task
from .models import TaskV1
from .models import WorkflowTask
from .runner_functions_low_level import _run_single_non_parallel_task
from .runner_functions_low_level import _run_single_parallel_task
from .runner_functions_low_level import _run_single_parallel_task_v1
from .task_interface import InitArgsModel
from .task_interface import InitTaskOutput
from .task_interface import TaskOutput

__all__ = [
    "run_non_parallel_task",
    "run_parallel_task",
    "run_compound_task",
    "run_parallel_task_v1",
]

MAX_PARALLELIZATION_LIST_SIZE = 200


def run_non_parallel_task(
    *,
    images: list[SingleImage],
    zarr_dir: str,
    task: Task,
    wftask: WorkflowTask,
    executor: Executor,
) -> TaskOutput:
    paths = [image.path for image in images]
    function_kwargs = dict(
        paths=paths,
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )
    future = executor.submit(
        functools.partial(_run_single_non_parallel_task, task=task),
        function_kwargs,
    )
    output = future.result()
    # FIXME V2: handle validation errors
    validated_output = TaskOutput(**output)
    return validated_output


def run_parallel_task(
    *,
    images: list[SingleImage],
    task: Task,
    wftask: WorkflowTask,
    executor: Executor,
) -> TaskOutput:
    if len(images) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(images)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    list_function_kwargs = [
        dict(
            path=image.path,
            **wftask.args_parallel,
        )
        for image in images
    ]
    results_iterator = executor.map(
        functools.partial(_run_single_parallel_task, task=task),
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


def run_compound_task(
    *,
    images: list[SingleImage],
    zarr_dir: str,
    task: Task,
    wftask: WorkflowTask,
    executor: Executor,
) -> TaskOutput:
    # 3/A: non-parallel init task
    paths = [image.path for image in images]
    function_kwargs = dict(
        paths=paths,
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )
    future = executor.submit(
        functools.partial(_run_single_non_parallel_task, task=task),
        function_kwargs,
    )
    output = future.result()
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
            )
        )
    results_iterator = executor.map(
        functools.partial(_run_single_parallel_task, task=task),
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


def run_parallel_task_v1(
    *,
    images: list[SingleImage],
    task: TaskV1,
    wftask: WorkflowTask,
    executor: Executor,
) -> TaskOutput:

    if len(images) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(images)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    list_function_kwargs = [
        _convert_v2_args_into_v1(
            dict(
                path=image.path,
                **wftask.args_parallel,
            )
        )
        for image in images
    ]

    results_iterator = executor.map(
        functools.partial(_run_single_parallel_task_v1, task=task),
        list_function_kwargs,
    )
    # Explicitly iterate over the whole list, so that all futures are waited
    list(results_iterator)

    # Ignore any output metadata for V1 tasks, and return an empty object
    return TaskOutput()
