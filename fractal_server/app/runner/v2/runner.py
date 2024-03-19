from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy

from ....images import _filter_image_list
from ....images import deduplicate_list
from ....images import find_image_by_path
from ....images import SingleImage
from .models import Dataset
from .models import DictStrAny
from .models import WorkflowTask
from .runner_functions import _run_compound_task
from .runner_functions import _run_non_parallel_task
from .runner_functions import _run_parallel_task


# FIXME: define RESERVED_ARGUMENTS = [", ...]


def _apply_attributes_to_image(
    *,
    image: SingleImage,
    new_attributes: DictStrAny,
    new_flags: DictStrAny,
) -> SingleImage:
    updated_image = copy(image)
    updated_image.attributes.update(new_attributes)
    updated_image.flags.update(new_flags)
    return updated_image


def execute_tasks_v2(
    wf_task_list: list[WorkflowTask],
    dataset: Dataset,
    executor: ThreadPoolExecutor,
) -> Dataset:

    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:

        task = wftask.task

        # PRE TASK EXECUTION

        # Get filtered images
        flag_filters = copy(dataset.flag_filters)
        flag_filters.update(wftask.input_flags)
        attribute_filters = copy(dataset.attribute_filters)
        attribute_filters.update(wftask.input_attributes)
        filtered_images = _filter_image_list(
            images=tmp_dataset.images,
            flag_filters=flag_filters,
            attribute_filters=attribute_filters,
        )
        # Verify that filtered images comply with output flags
        if not all(
            img.match_filter(flag_filters=task.input_flags)
            for img in filtered_images
        ):
            raise ValueError(
                "Filtered images do not comply with task input_flags."
            )

        # ACTUAL TASK EXECUTION

        # Non-parallel task
        if task.task_type == "non_parallel_standalone":
            task_output = _run_non_parallel_task(
                filtered_images=filtered_images,
                zarr_dir=tmp_dataset.zarr_dir,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        # Parallel task
        elif task.task_type == "parallel_standalone":
            task_output = _run_parallel_task(
                filtered_images=filtered_images,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        # Compound task
        elif task.task_type == "compound":
            task_output = _run_compound_task(
                filtered_images=filtered_images,
                zarr_dir=tmp_dataset.zarr_dir,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        else:
            raise ValueError(f"Invalid {task.task_type=}.")

        # POST TASK EXECUTION

        # Propagate attributes and flags from `origin` to added_images
        added_images = task_output.added_images
        for ind, img in enumerate(added_images):
            if img.origin is None:
                continue
            original_img = find_image_by_path(
                images=tmp_dataset.images,
                path=img.origin,
            )
            if original_img is None:
                continue
            updated_attributes = copy(original_img.attributes)
            updated_attributes.update(img.attributes)
            updated_flags = copy(original_img.flags)
            updated_flags.update(img.flags)
            added_images[ind] = SingleImage(
                path=img.path,
                attributes=updated_attributes,
                flags=updated_flags,
                origin=img.origin,
            )
        task_output.added_images = added_images

        # Construct up-to-date flag filters
        # TODO extract as a helper function
        if task.output_flags is not None:
            flags_from_task_manifest = set(task.output_flags.keys())
            flags_from_task_execution = set(
                task_output.new_flag_filters.keys()
            )
            if not flags_from_task_manifest.isdisjoint(
                flags_from_task_execution
            ):
                overlap = flags_from_task_manifest.intersection(
                    flags_from_task_execution
                )
                raise ValueError(
                    "Both task and task manifest did set the same"
                    f"output flag. Overlapping keys: {overlap}."
                )
        new_flag_filters = copy(tmp_dataset.flag_filters)
        new_flag_filters.update(task_output.new_flag_filters)
        if task.output_flags is not None:
            new_flag_filters.update(task.output_flags)

        # Construct up-to-date attribute filters
        new_attribute_filters = copy(tmp_dataset.attribute_filters) or {}
        new_attribute_filters.update(task_output.new_attribute_filters)

        # Add filters to edited images, and update Dataset.images
        edited_images = task_output.edited_images
        edited_paths = [image.path for image in edited_images]
        for ind, image in enumerate(tmp_dataset.images):
            if image.path in edited_paths:
                updated_image = _apply_attributes_to_image(
                    image=image,
                    new_attributes=new_attribute_filters,
                    new_flags=new_flag_filters,
                )
                tmp_dataset.images[ind] = updated_image

        # Add filters to new images
        added_images = task_output.added_images
        for ind, image in enumerate(added_images):
            updated_image = _apply_attributes_to_image(
                image=image,
                new_attributes=new_attribute_filters,
                new_flags=new_flag_filters,
            )
            added_images[ind] = updated_image
        added_images = deduplicate_list(added_images)

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
            removed_image.path for removed_image in task_output.removed_images
        ]

        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image.path not in removed_images_paths
        ]

        # Update Dataset.filters
        tmp_dataset.attribute_filters = new_attribute_filters
        tmp_dataset.flag_filters = new_flag_filters

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
