from concurrent.futures import ThreadPoolExecutor
from copy import copy
from typing import Any

from ....images import find_image_by_path
from ....images import SingleImage
from .models import DictStrAny
from .models import Task
from .task_output import InitTaskOutput
from .task_output import TaskOutput

MAX_PARALLELIZATION_LIST_SIZE = 200


def _run_non_parallel_task(
    task: Task,
    function_kwargs: DictStrAny,
    old_dataset_images: list[SingleImage],
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

    task_output = update_task_output_added_images(
        task_output=task_output,
        old_dataset_images=old_dataset_images,
        task_input_paths=function_kwargs["paths"],
    )

    return TaskOutput(**task_output.dict())


def _run_non_parallel_task_init(
    task: Task,
    function_kwargs: DictStrAny,
    old_dataset_images: list[SingleImage],
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


def update_task_output_added_images(
    task_output: TaskOutput,
    task_input_paths: list[str],
    old_dataset_images: list[SingleImage],
):
    """
    Update task_output.added_images (if set)
    by propagating old images attributes
    """
    if task_output.added_images is None:
        return task_output

    added_image_paths = [
        added_image.path for added_image in task_output.added_images
    ]
    if len(task_input_paths) == len(added_image_paths):
        final_added_images = []
        for ind, added_image in enumerate(task_output.added_images):
            old_image = find_image_by_path(
                images=old_dataset_images,
                path=task_input_paths[ind],
            )
            added_image.attributes = (
                old_image.attributes | added_image.attributes
            )
            final_added_images.append(added_image)
        task_output.added_images = final_added_images
    elif len(task_input_paths) == 0 or len(added_image_paths) == 0:
        pass
    else:
        print("WARNING: If lenghts are different we don't know how to proceed")
    return task_output


def merge_outputs(
    task_outputs: list[TaskOutput],
    new_old_image_mapping: dict[str, str],
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    final_added_images = []
    final_edited_images = []
    final_removed_images = []

    for ind, task_output in enumerate(task_outputs):

        if task_output.added_images:
            for added_image in task_output.added_images:
                old_image = find_image_by_path(
                    images=old_dataset_images,
                    path=new_old_image_mapping[added_image.path],
                )
                # Propagate old-image attributes to new-image
                if old_image is not None:
                    added_image.attributes = (
                        old_image.attributes | added_image.attributes
                    )
                final_added_images.append(added_image)

        if task_output.edited_images:
            for edited_image in task_output.edited_images:
                final_edited_images.append(edited_image)

        if task_output.removed_images:
            for removed_image in task_output.removed_images:
                final_removed_images.append(removed_image)

        # check that all filters are the same
        current_new_filters = task_output.new_filters
        if ind == 0:
            last_new_filters = copy(current_new_filters)
        if current_new_filters != last_new_filters:
            raise ValueError(f"{current_new_filters=} but {last_new_filters=}")

        last_new_filters = copy(current_new_filters)

    final_output = TaskOutput(
        added_images=final_added_images,
        edited_images=final_edited_images,
        new_filters=last_new_filters,
        removed_images=final_removed_images,
    )

    return final_output


def _run_parallel_task(
    task: Task,
    list_function_kwargs: list[DictStrAny],
    old_dataset_images: list[SingleImage],
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

    new_old_image_mapping = {}

    for ind, task_output in enumerate(task_outputs):
        if task_output.added_images is not None:
            for added_image in task_output.added_images:
                new_key = added_image.path
                new_value = list_function_kwargs[ind]["path"]
                old_value = new_old_image_mapping.get(new_key)
                if old_value is not None and old_value != new_value:
                    raise ValueError(
                        "The same `added_image.path` corresponds to "
                        "multiple `path` function arguments. This means "
                        "that two tasks with different `path` input created "
                        "the same `added_image` entry."
                    )
                new_old_image_mapping[new_key] = new_value

    merged_output = merge_outputs(
        task_outputs,
        new_old_image_mapping,
        old_dataset_images,
    )
    return merged_output
