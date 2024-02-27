from copy import copy

from .images import find_image_by_path
from .models import DictStrAny
from .models import SingleImage
from .models import Task
from .task_output import ParallelTaskOutput
from .task_output import TaskOutput

MAX_PARALLELIZATION_LIST_SIZE = 200


def _run_non_parallel_task(
    task: Task,
    function_kwargs: DictStrAny,
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    task_output = task.function(**function_kwargs)

    if task_output is None:
        return TaskOutput()

    task_output = TaskOutput(**task_output)

    task_output = update_task_output_new_images(
        task_output=task_output,
        old_dataset_images=old_dataset_images,
        task_input_paths=function_kwargs["paths"],
    )

    return TaskOutput(**task_output.dict())


def update_task_output_new_images(
    task_output: TaskOutput,
    task_input_paths: list[str],
    old_dataset_images: list[SingleImage],
):
    """
    Update task_output.new_images (if set)
    by propagating old images attributes
    """
    if task_output.new_images is None:
        return task_output

    new_image_paths = [new_image.path for new_image in task_output.new_images]
    if len(task_input_paths) == len(new_image_paths):
        final_new_images = []
        for ind, new_image in enumerate(task_output.new_images):
            old_image = find_image_by_path(
                images=old_dataset_images,
                path=task_input_paths[ind],
            )
            new_image.attributes = old_image.attributes | new_image.attributes
            final_new_images.append(new_image)
        task_output.new_images = final_new_images
    else:
        print("WARNING: If lenghts are different we don't know how to proceed")
    return task_output


def merge_outputs(
    task_outputs: list[ParallelTaskOutput],
    new_old_image_mapping: dict[str, str],
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    final_new_images = []
    final_edited_images = []
    final_removed_images = []

    for ind, task_output in enumerate(task_outputs):

        if task_output.new_images:
            for new_image in task_output.new_images:
                old_image = find_image_by_path(
                    images=old_dataset_images,
                    path=new_old_image_mapping[new_image.path],
                )
                # Propagate old-image attributes to new-image
                new_image.attributes = (
                    old_image.attributes | new_image.attributes
                )
                final_new_images.append(new_image)

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
        new_images=final_new_images,
        edited_images=final_edited_images,
        new_filters=last_new_filters,
        removed_images=final_removed_images,
    )

    return final_output


def _run_parallel_task(
    task: Task,
    list_function_kwargs: list[DictStrAny],
    old_dataset_images: list[SingleImage],
) -> DictStrAny:

    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    task_outputs = []
    new_old_image_mapping = {}
    for function_kwargs in list_function_kwargs:

        # FIXME functools.partial
        task_output = task.function(**function_kwargs)
        if task_output is None:
            task_output = ParallelTaskOutput()
        else:
            task_output = ParallelTaskOutput(**task_output)

        task_outputs.append(copy(task_output))

        if task_output.new_images is not None:
            # FIXME check keys are not repeated
            new_old_image_mapping.update(
                {
                    new_image.path: function_kwargs["path"]
                    for new_image in task_output.new_images
                }
            )

    merged_output = merge_outputs(
        task_outputs,
        new_old_image_mapping,
        old_dataset_images,
    )
    return merged_output
