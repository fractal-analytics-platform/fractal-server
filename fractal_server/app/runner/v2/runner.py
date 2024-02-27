from copy import copy
from copy import deepcopy

from .images import _deduplicate_list
from .images import filter_images
from .images import find_image_by_path
from .images import SingleImage
from .models import Dataset
from .models import DictStrAny
from .models import WorkflowTask
from .runner_functions import _run_non_parallel_task
from .runner_functions import _run_parallel_task


# FIXME: define RESERVED_ARGUMENTS = ["buffer", ...]


def _apply_attributes_to_image(
    *,
    image: SingleImage,
    new_attributes: DictStrAny,
) -> SingleImage:
    updated_image = copy(image)
    updated_image.attributes.update(new_attributes)
    return updated_image


def _validate_parallelization_list_valid(
    parallelization_list: list[DictStrAny],
    current_image_paths: list[SingleImage],
) -> None:
    for kwargs in parallelization_list:
        path = kwargs.get("path")
        if path is None:
            raise ValueError(
                "An element in parallelization list has no path:\n"
                f"{kwargs=}"
            )
        if path not in current_image_paths:
            raise ValueError(
                "An element in parallelization list does not match "
                f"with any image:\n{kwargs=}"
            )
        if "buffer" in kwargs.keys():
            raise ValueError(
                f"An element in parallelization list is not valid:\n{kwargs=}"
            )


def execute_tasks_v2(
    wf_task_list: list[WorkflowTask],
    dataset: Dataset,
) -> Dataset:

    # Run task 0
    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:
        task = wftask.task

        # Extract tmp_buffer
        if tmp_dataset.buffer is not None:
            tmp_buffer = tmp_dataset.buffer
        else:
            tmp_buffer = {}

        # (1/2) Non-parallel task
        if task.task_type == "non_parallel":
            if tmp_dataset.parallelization_list is not None:
                raise ValueError(
                    "Found parallelization_list for non-parallel task"
                )
            else:
                # Get filtered images
                current_task_images = filter_images(
                    dataset_images=tmp_dataset.images,
                    dataset_filters=tmp_dataset.filters,
                    wftask_filters=wftask.filters,
                )
                paths = [image.path for image in current_task_images]
                function_kwargs = dict(
                    paths=paths,
                    buffer=tmp_buffer,
                    **wftask.args,
                )
                task_output = _run_non_parallel_task(
                    task=task,
                    function_kwargs=function_kwargs,
                    old_dataset_images=current_task_images,
                )
        # (2/2) Parallel task
        elif task.task_type == "parallel":
            # Prepare list_function_kwargs
            if tmp_dataset.parallelization_list is None:
                # Get filtered images
                current_task_images = filter_images(
                    dataset_images=tmp_dataset.images,
                    dataset_filters=tmp_dataset.filters,
                    wftask_filters=wftask.filters,
                )
                list_function_kwargs = [
                    dict(path=image.path, buffer=tmp_buffer, **wftask.args)
                    for image in current_task_images
                ]

            else:
                # Use pre-made parallelization_list
                parallelization_list = tmp_dataset.parallelization_list

                _validate_parallelization_list_valid(
                    parallelization_list=parallelization_list,
                    current_image_paths=tmp_dataset.image_paths,
                )
                list_function_kwargs = deepcopy(parallelization_list)
                for ind, kwargs in enumerate(list_function_kwargs):
                    list_function_kwargs[ind].update(
                        dict(
                            buffer=tmp_buffer,
                            **wftask.args,
                        )
                    )
                list_function_kwargs = _deduplicate_list(list_function_kwargs)

                current_task_images = [
                    find_image_by_path(
                        images=tmp_dataset.images, path=kwargs["path"]
                    )
                    for kwargs in list_function_kwargs
                ]

                current_task_images = _deduplicate_list(current_task_images)

            task_output = _run_parallel_task(
                task=task,
                list_function_kwargs=list_function_kwargs,
                old_dataset_images=current_task_images,
            )
        else:
            raise ValueError(f"Invalid {task.task_type=}.")

        # Construct up-to-date filters
        new_filters = copy(tmp_dataset.filters)
        new_filters.update(task.new_filters)
        if task_output.new_filters is not None:
            new_filters.update(task_output.new_filters)

        # Add filters to edited images, and update Dataset.images
        edited_images = task_output.edited_images or []
        edited_paths = [image.path for image in edited_images]
        for ind, image in enumerate(tmp_dataset.images):
            if image.path in edited_paths:
                updated_image = _apply_attributes_to_image(
                    image=image, new_attributes=new_filters
                )
                tmp_dataset.images[ind] = updated_image
        # Add filters to new images
        added_images = task_output.added_images or []
        for ind, image in enumerate(added_images):
            updated_image = _apply_attributes_to_image(
                image=image, new_attributes=new_filters
            )
            added_images[ind] = updated_image
        added_images = _deduplicate_list(added_images)

        # Get removed images
        removed_images = task_output.removed_images or []

        # Add new images to Dataset.images
        for image in added_images:
            if image.path in tmp_dataset.image_paths:
                raise ValueError("Found an overlap")

            tmp_dataset.images.append(image)

        # Remove images from Dataset.images
        removed_images_paths = [
            removed_image.path for removed_image in removed_images
        ]

        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image.path not in removed_images_paths
        ]
        # Update Dataset.filters
        tmp_dataset.filters = new_filters

        # Update Dataset.buffer
        tmp_dataset.buffer = task_output.buffer

        # Update Dataset.parallelization_list
        # NOTE: this mut be done *after* Dataset.images was updated
        new_parallelization_list = task_output.parallelization_list
        if new_parallelization_list is not None:
            _validate_parallelization_list_valid(
                parallelization_list=new_parallelization_list,
                current_image_paths=tmp_dataset.image_paths,
            )
        tmp_dataset.parallelization_list = new_parallelization_list

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
