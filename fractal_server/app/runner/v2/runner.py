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
        flag_filters = copy(dataset.filters.flags)
        flag_filters.update(wftask.filters.flags)
        attribute_filters = copy(dataset.filters.attributes)
        attribute_filters.update(wftask.filters.attributes)
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

        # Update image list
        current_task_output.check_paths_are_unique()
        for image in current_task_output.image_list_updates:
            # Edit existing image
            if image.path in tmp_dataset.image_paths:
                if image.origin is not None and image.origin != image.path:
                    raise ValueError(
                        f"Trying to edit an image with {image.path=} "
                        f"and {image.origin=}."
                    )
                image_search = find_image_by_path(
                    images=tmp_dataset.images,
                    path=image.path,
                )
                if image_search is None:
                    raise ValueError("This should have not happened")
                original_img = image_search["image"]
                original_index = image_search["index"]
                updated_attributes = copy(original_img.attributes)
                updated_flags = copy(original_img.flags)

                # Update image attributes/flags with task output and manifest
                updated_attributes.update(image.attributes)
                updated_flags.update(image.flags)
                updated_flags.update(task.output_flags)

                # Update image in the dataset image list
                tmp_dataset.images[
                    original_index
                ].attributes = updated_attributes
                tmp_dataset.images[original_index].flags = updated_flags
            # Add new image
            else:
                # Check that image.path is relative to zarr_dir
                if not image.path.startswith(tmp_dataset.zarr_dir):
                    raise ValueError(
                        f"{tmp_dataset.zarr_dir} is not a parent directory of "
                        f"{image.path}"
                    )
                # Propagate attributes and flags from `origin` (if any)
                updated_attributes = {}
                updated_flags = {}
                if image.origin is not None:
                    image_search = find_image_by_path(
                        images=tmp_dataset.images,
                        path=image.origin,
                    )
                    if image_search is not None:
                        original_img = image_search["image"]
                        updated_attributes = copy(original_img.attributes)
                        updated_flags = copy(original_img.flags)
                # Update image attributes/flags with task output and manifest
                updated_attributes.update(image.attributes)
                updated_flags.update(image.flags)
                updated_flags.update(task.output_flags)
                new_image = SingleImage(
                    path=image.path,
                    origin=image.origin,
                    attributes=updated_attributes,
                    flags=updated_flags,
                )
                # Add image into the dataset image list
                tmp_dataset.images.append(new_image)

        # Remove images from Dataset.images
        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image.path not in current_task_output.image_list_removals
        ]

        # Update Dataset.attribute_filters:
        # current + (task_output: not really, in current examples..)
        if current_task_output.filters is not None:
            tmp_dataset.filters.attributes.update(
                current_task_output.filters.attributes
            )

        # Update Dataset.flag_filters: current + (task_output + task_manifest)
        flags_from_manifest = task.output_flags
        if current_task_output.filters is not None:
            flags_from_task = current_task_output.filters.flags
        else:
            flags_from_task = {}
        # Check that key sets are disjoint
        set_flags_from_manifest = set(flags_from_manifest.keys())
        set_flags_from_task = set(flags_from_task.keys())
        if not set_flags_from_manifest.isdisjoint(set_flags_from_task):
            overlap = set_flags_from_manifest.intersection(set_flags_from_task)
            raise ValueError(
                "Both task and task manifest did set the same"
                f"output flag. Overlapping keys: {overlap}."
            )
        # Update Dataset.flag_filters
        tmp_dataset.filters.flags.update(flags_from_manifest)
        tmp_dataset.filters.flags.update(flags_from_task)

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
