from copy import copy

from .env import MAX_PARALLELIZATION_LIST_SIZE
from .images import find_image_by_path
from .models import DictStrAny
from .models import SingleImage
from .models import Task
from .task_output import ParallelTaskOutput
from .task_output import TaskOutput


def _run_non_parallel_task(
    task: Task,
    function_kwargs: DictStrAny,
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    task_output = task.function(**function_kwargs)

    if task_output is None:
        task_output = TaskOutput()
    else:
        task_output = TaskOutput(**task_output)

    # Process the output, to propagate some image attributes - if possible
    # FIXME: refactor this into a "process_task_output" function?
    if task_output.new_images is not None:
        old_image_paths = function_kwargs["paths"]
        new_image_paths = [
            new_image.path for new_image in task_output.new_images
        ]
        if len(old_image_paths) == len(new_image_paths):
            new_old_image_mapping = {}
            for ind, new_image_path in enumerate(new_image_paths):
                new_old_image_mapping[new_image_path] = old_image_paths[ind]
            final_new_images = []
            for new_image in task_output.new_images:
                old_image = find_image_by_path(
                    images=old_dataset_images,
                    path=new_old_image_mapping[new_image.path],
                )
                new_image.attributes = (
                    old_image.attributes | new_image.attributes
                )
                final_new_images.append(new_image)
            task_output.new_images = final_new_images

    return TaskOutput(**task_output.dict())


def merge_outputs(
    task_outputs: list[ParallelTaskOutput],
    new_old_image_mapping: dict[str, str],
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    final_new_images = []
    final_edited_images = []
    final_removed_images = []
    final_new_filters = None

    for task_output in task_outputs:

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

        new_filters = task_output.new_filters
        if new_filters:
            if final_new_filters is None:
                final_new_filters = new_filters
            else:
                if final_new_filters != new_filters:
                    raise ValueError(
                        f"{new_filters=} but {final_new_filters=}"
                    )

    final_output = TaskOutput()
    if final_new_images:
        final_output.new_images = final_new_images
    if final_edited_images:
        final_output.edited_images = final_edited_images
    if final_new_filters:
        final_output.new_filters = final_new_filters
    if final_edited_images:
        final_output.removed_images = final_removed_images

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

        task_output = task.function(**function_kwargs)
        if task_output is None:
            task_output = ParallelTaskOutput()
        else:
            task_output = ParallelTaskOutput(**task_output)

        if task_output.new_images is not None:
            new_old_image_mapping.update(
                {
                    new_image.path: function_kwargs["path"]
                    for new_image in task_output.new_images
                }
            )

        task_outputs.append(copy(task_output))

    merged_output = merge_outputs(
        task_outputs,
        new_old_image_mapping,
        old_dataset_images,
    )
    return merged_output
