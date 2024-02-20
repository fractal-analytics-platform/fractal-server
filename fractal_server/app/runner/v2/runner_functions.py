from copy import copy

from .env import MAX_PARALLELIZATION_LIST_SIZE
from .images import find_image_by_path
from .models import KwargsType
from .models import SingleImage
from .models import Task
from .task_output import merge_outputs
from .task_output import ParallelTaskOutput
from .task_output import TaskOutput
from .utils import pjson


def _run_non_parallel_task(
    task: Task,
    function_kwargs: KwargsType,
    old_dataset_images: list[SingleImage],
) -> KwargsType:

    task_output = task.function(**function_kwargs)
    if task_output is None:
        task_output = {}

    # Process the output, to propagate some image attributes - if possible
    # FIXME: refactor this into a "process_task_output" function?
    if task_output.get("new_images") is not None:
        old_image_paths = function_kwargs["paths"]
        new_image_paths = [
            new_image.path for new_image in task_output.get("new_images")
        ]
        if len(old_image_paths) == len(new_image_paths):
            new_old_image_mapping = {}
            for ind, new_image_path in enumerate(new_image_paths):
                new_old_image_mapping[new_image_path] = old_image_paths[ind]
            final_new_images = []
            for new_image in task_output.get("new_images"):
                old_image = find_image_by_path(
                    images=old_dataset_images,
                    path=new_old_image_mapping[new_image.path],
                )
                new_image.attributes = (
                    old_image.attributes | new_image.attributes
                )
                final_new_images.append(new_image)
            task_output["new_images"] = final_new_images

    print(f"Task output:\n{pjson(task_output)}")

    return TaskOutput(**task_output)


def _run_parallel_task(
    task: Task,
    list_function_kwargs: list[KwargsType],
    old_dataset_images: list[SingleImage],
) -> KwargsType:

    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    task_outputs = []
    new_old_image_mapping = {}
    for function_kwargs in list_function_kwargs:

        task_output = task.function(**function_kwargs) or {}

        if task_output.get("new_images") is not None:
            new_old_image_mapping.update(
                {
                    new_image.path: function_kwargs["path"]
                    for new_image in task_output["new_images"]
                }
            )

        ParallelTaskOutput(**task_output)
        task_outputs.append(copy(task_output))

    merged_output = merge_outputs(
        task_outputs,
        new_old_image_mapping,
        old_dataset_images,
    )

    return TaskOutput(**merged_output)
