from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy

from ....images import deduplicate_list
from ....images import filter_images
from ....images import find_image_by_path
from ....images import SingleImage
from .models import Dataset
from .models import DictStrAny
from .models import WorkflowTask
from .runner_functions import _run_non_parallel_task
from .runner_functions import _run_non_parallel_task_init
from .runner_functions import _run_parallel_task


# FIXME: define RESERVED_ARGUMENTS = [", ...]


def _apply_attributes_to_image(
    *,
    image: SingleImage,
    new_attributes: DictStrAny,
) -> SingleImage:
    updated_image = copy(image)
    updated_image.attributes.update(new_attributes)
    return updated_image


def execute_tasks_v2(
    wf_task_list: list[WorkflowTask],
    dataset: Dataset,
    executor: ThreadPoolExecutor,
) -> Dataset:

    # Run task 0
    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:
        task = wftask.task

        # Get filtered images
        filtered_images = filter_images(
            dataset_images=tmp_dataset.images,
            dataset_filters=tmp_dataset.filters,
            wftask_filters=wftask.filters,
        )

        # (1/3) Non-parallel task
        if task.task_type == "non_parallel_standalone":
            paths = [image.path for image in filtered_images]
            function_kwargs = dict(
                paths=paths,
                zarr_dir=tmp_dataset.zarr_dir,
                **wftask.args_non_parallel,
            )
            task_output = _run_non_parallel_task(
                task=task,
                function_kwargs=function_kwargs,
                old_dataset_images=filtered_images,
                executor=executor,
            )
        # (2/3) Parallel task
        elif task.task_type == "parallel_standalone":
            list_function_kwargs = [
                dict(
                    path=image.path,
                    **wftask.args_parallel,
                )
                for image in filtered_images
            ]
            task_output = _run_parallel_task(
                task=task,
                list_function_kwargs=list_function_kwargs,
                # old_dataset_images=filtered_images,
                executor=executor,
            )
        # (3/3) Compound task
        elif task.task_type == "compound":
            # 3/A: non-parallel init task
            paths = [image.path for image in filtered_images]
            function_kwargs = dict(
                paths=paths,
                zarr_dir=tmp_dataset.zarr_dir,
                **wftask.args_non_parallel,
            )
            init_task_output = _run_non_parallel_task_init(
                task=task,
                function_kwargs=function_kwargs,
                old_dataset_images=filtered_images,
                executor=executor,
            )

            # 3/B: parallel part of a compound task
            parallelization_list = init_task_output.parallelization_list
            list_function_kwargs = []
            for ind, parallelization_item in enumerate(parallelization_list):
                list_function_kwargs.append(
                    dict(
                        path=parallelization_item.path,
                        init_args=parallelization_item.init_args,
                        **wftask.args_parallel,
                    )
                )
            list_function_kwargs = deduplicate_list(list_function_kwargs)
            these_filtered_images = [
                find_image_by_path(
                    images=tmp_dataset.images, path=kwargs["path"]
                )
                for kwargs in list_function_kwargs
            ]
            these_filtered_images = deduplicate_list(
                these_filtered_images, remove_None=True
            )
            task_output = _run_parallel_task(
                task=task,
                list_function_kwargs=list_function_kwargs,
                # old_dataset_images=these_filtered_images,
                executor=executor,
            )
        else:
            raise ValueError(f"Invalid {task.task_type=}.")

        # Propagate attributes from `origin` to added_images
        added_images = task_output.added_images or []
        for ind, img in enumerate(added_images):
            if "origin" not in img.attributes.keys():
                continue
            original_path = img.attributes["origin"]
            original_img = find_image_by_path(
                images=tmp_dataset.images, path=original_path
            )
            if original_img is not None:
                updated_attributes = copy(original_img.attributes)
                updated_attributes.update(img.attributes)
                added_images[ind] = SingleImage(
                    path=img.path, attributes=updated_attributes
                )
        task_output.added_images = added_images

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
        added_images = deduplicate_list(added_images)

        # Get removed images
        removed_images = task_output.removed_images or []

        # Add new images to Dataset.images
        for image in added_images:
            if image.path in tmp_dataset.image_paths:
                raise ValueError("Found an overlap")
            if not image.path.startswith(tmp_dataset.zarr_dir):
                raise ValueError(
                    f"'{tmp_dataset.zarr_dir}' is not a parent directory of "
                    f"'{image.path}'"
                )

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

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
