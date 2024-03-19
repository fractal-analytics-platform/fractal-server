from concurrent.futures import ThreadPoolExecutor
from copy import copy
from typing import Any

from .models import DictStrAny
from .models import Task
from .task_output import InitTaskOutput
from .task_output import TaskOutput


MAX_PARALLELIZATION_LIST_SIZE = 200


def _run_non_parallel_task(
    task: Task,
    function_kwargs: DictStrAny,
    executor: ThreadPoolExecutor,
) -> TaskOutput:
    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_non_parallel(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    task_output = executor.submit(
        _wrapper_expand_kwargs, function_kwargs
    ).result()

    return TaskOutput(**task_output.dict())


def _run_non_parallel_task_init(
    task: Task,
    function_kwargs: DictStrAny,
    executor: ThreadPoolExecutor,
) -> InitTaskOutput:
    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_non_parallel(**input_kwargs)
        if task_output is None:
            task_output = InitTaskOutput()
        else:
            task_output = InitTaskOutput(**task_output)
        return task_output

    task_output = executor.submit(
        _wrapper_expand_kwargs, function_kwargs
    ).result()

    return InitTaskOutput(**task_output.dict())


def merge_outputs(
    task_outputs: list[TaskOutput],
) -> TaskOutput:

    final_added_images = []
    final_edited_images = []
    final_removed_images = []

    for ind, task_output in enumerate(task_outputs):

        if task_output.added_images:
            for added_image in task_output.added_images:
                final_added_images.append(added_image)

        if task_output.edited_images:
            for edited_image in task_output.edited_images:
                final_edited_images.append(edited_image)

        if task_output.removed_images:
            for removed_image in task_output.removed_images:
                final_removed_images.append(removed_image)

        # Check that all attribute_filters are the same
        current_new_attribute_filters = task_output.new_attribute_filters
        if ind == 0:
            last_new_attribute_filters = copy(current_new_attribute_filters)
        if current_new_attribute_filters != last_new_attribute_filters:
            raise ValueError(
                f"{current_new_attribute_filters=} but "
                f"{last_new_attribute_filters=}"
            )
        last_new_attribute_filters = copy(current_new_attribute_filters)

        # Check that all flag_filters are the same
        current_new_flag_filters = task_output.new_flag_filters
        if ind == 0:
            last_new_flag_filters = copy(current_new_flag_filters)
        if current_new_flag_filters != last_new_flag_filters:
            raise ValueError(
                f"{current_new_flag_filters=} but " f"{last_new_flag_filters=}"
            )
        last_new_flag_filters = copy(current_new_flag_filters)

    final_output = TaskOutput(
        added_images=final_added_images,
        edited_images=final_edited_images,
        new_attribute_filters=last_new_attribute_filters,
        new_flag_filters=last_new_flag_filters,
        removed_images=final_removed_images,
    )

    return final_output


def _run_parallel_task(
    task: Task,
    list_function_kwargs: list[DictStrAny],
    executor: ThreadPoolExecutor,
) -> TaskOutput:

    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_parallel(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    results = executor.map(_wrapper_expand_kwargs, list_function_kwargs)
    task_outputs = list(results)

    merged_output = merge_outputs(
        task_outputs,
    )
    return merged_output
