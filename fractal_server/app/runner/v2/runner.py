from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy

from ....images import _filter_image_list
from ....images import find_image_by_path
from ....images import SingleImage
from .models import Dataset
from .models import DictStrAny
from .models import WorkflowTask
from .runner_functions import _run_compound_task
from .runner_functions import _run_non_parallel_task
from .runner_functions import _run_parallel_task
from fractal_server.app.runner.v2.runner_functions import deduplicate_list


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
        flag_filters.update(wftask.flag_filters)
        attribute_filters = copy(dataset.attribute_filters)
        attribute_filters.update(wftask.attribute_filters)
        filtered_images = _filter_image_list(
            images=tmp_dataset.images,
            flag_filters=flag_filters,
            attribute_filters=attribute_filters,
        )
        # Verify that filtered images comply with output flags
        for image in filtered_images:
            if not image.match_filter(flag_filters=task.input_flags):
                raise ValueError(
                    f"Filtered images include {image.dict()}, which does "
                    f"not comply with {task.input_flags=}."
                )

        # ACTUAL TASK EXECUTION

        # Non-parallel task
        if task.task_type == "non_parallel_standalone":
            current_task_output = _run_non_parallel_task(
                filtered_images=filtered_images,
                zarr_dir=tmp_dataset.zarr_dir,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        # Parallel task
        elif task.task_type == "parallel_standalone":
            current_task_output = _run_parallel_task(
                filtered_images=filtered_images,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        # Compound task
        elif task.task_type == "compound":
            current_task_output = _run_compound_task(
                filtered_images=filtered_images,
                zarr_dir=tmp_dataset.zarr_dir,
                wftask=wftask,
                task=wftask.task,
                executor=executor,
            )
        else:
            raise ValueError(f"Invalid {task.task_type=}.")

        # POST TASK EXECUTION

        # Construct up-to-date flag filters
        # TODO extract as a helper function
        if task.output_flags is not None:
            flags_from_task_manifest = set(task.output_flags.keys())
            flags_from_task_execution = set(current_task_output.flags.keys())
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
        new_flags = copy(tmp_dataset.flag_filters)
        new_flags.update(current_task_output.flags)
        if task.output_flags is not None:
            new_flags.update(task.output_flags)

        # Construct up-to-date attribute filters
        new_attributes = copy(tmp_dataset.attribute_filters) or {}
        new_attributes.update(current_task_output.attributes)

        # Add filters to edited images, and update Dataset.images
        for ind, image in enumerate(tmp_dataset.images):
            if image.path in current_task_output.edited_image_paths:
                updated_image = _apply_attributes_to_image(
                    image=image,
                    new_attributes=new_attributes,
                    new_flags=new_flags,
                )
                tmp_dataset.images[ind] = updated_image

        # Create clean added_images list
        added_images = deepcopy(current_task_output.added_images)
        for ind, image in enumerate(added_images):
            # Check that image was not already present
            if image.path in tmp_dataset.image_paths:
                raise ValueError("Found an overlap")

            # Check that image.path is relative to zarr_dir
            if not image.path.startswith(tmp_dataset.zarr_dir):
                raise ValueError(
                    f"'{tmp_dataset.zarr_dir}' is not a parent directory of "
                    f"'{image.path}'"
                )

            # Propagate attributes and flags from `origin` to added_images
            if image.origin is not None:
                original_img = find_image_by_path(
                    images=tmp_dataset.images,
                    path=image.origin,
                )
                if original_img is not None:
                    updated_attributes = copy(original_img.attributes)
                    updated_attributes.update(image.attributes)
                    updated_flags = copy(original_img.flags)
                    updated_flags.update(image.flags)
                    added_images[ind] = SingleImage(
                        path=image.path,
                        origin=image.origin,
                        attributes=updated_attributes,
                        flags=updated_flags,
                    )

            # Apply new attributes/flags to image
            updated_image = _apply_attributes_to_image(
                image=added_images[ind],
                new_attributes=new_attributes,
                new_flags=new_flags,
            )
            added_images[ind] = updated_image

        # Deduplicate new image list
        added_images = deduplicate_list(added_images)

        # Add new images to Dataset.images
        tmp_dataset.images.extend(added_images)

        # Remove images from Dataset.images
        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image.path not in current_task_output.removed_image_paths
        ]

        # Update Dataset.filters
        tmp_dataset.attribute_filters = copy(new_attributes)
        tmp_dataset.flag_filters = copy(new_flags)

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
