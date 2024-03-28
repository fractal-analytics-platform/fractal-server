from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Optional

from ....images import Filters
from ....images import SingleImage
from ....images.tools import _filter_image_list
from ....images.tools import find_image_by_path
from ....images.tools import match_filter
from .runner_functions import run_compound_task
from .runner_functions import run_non_parallel_task
from .runner_functions import run_parallel_task
from .runner_functions import run_parallel_task_v1
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import WorkflowTaskV2

# FIXME: define RESERVED_ARGUMENTS = [", ...]


def execute_tasks_v2(
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    executor: ThreadPoolExecutor,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
) -> DatasetV2:
    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:
        task = wftask.task

        # PRE TASK EXECUTION

        # Get filtered images
        type_filters = copy(dataset.filters.types)
        type_filters.update(wftask.filters.types)
        attribute_filters = copy(dataset.filters.attributes)
        attribute_filters.update(wftask.filters.attributes)
        filtered_images = _filter_image_list(
            images=tmp_dataset.images,
            filters=Filters(types=type_filters, attributes=attribute_filters),
        )
        if wftask.is_legacy_task:

            current_task_output = run_parallel_task_v1(
                images=filtered_images,
                task=wftask.task,
                wftask=wftask,
                executor=executor,
            )

        else:
            # Verify that filtered images comply with output types
            for image in filtered_images:
                if not match_filter(image, Filters(types=task.input_types)):
                    raise ValueError(
                        f"Filtered images include {image.dict()}, which does "
                        f"not comply with {task.input_types=}."
                    )

            # ACTUAL TASK EXECUTION

            # Non-parallel task
            if task.type == "non_parallel_standalone":
                current_task_output = run_non_parallel_task(
                    images=filtered_images,
                    zarr_dir=tmp_dataset.zarr_dir,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                )
            # Parallel task
            elif task.type == "parallel_standalone":
                current_task_output = run_parallel_task(
                    images=filtered_images,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                )
            # Compound task
            elif task.type == "compound":
                current_task_output = run_compound_task(
                    images=filtered_images,
                    zarr_dir=tmp_dataset.zarr_dir,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                )
            else:
                raise ValueError(f"Invalid {task.task_type=}.")

        # POST TASK EXECUTION
        from devtools import debug

        debug("POST TASK EXECUTION", current_task_output)
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
                updated_types = copy(original_img.types)

                # Update image attributes/types with task output and manifest
                updated_attributes.update(image.attributes)
                updated_types.update(image.types)
                updated_types.update(task.output_types)

                # Update image in the dataset image list
                tmp_dataset.images[
                    original_index
                ].attributes = updated_attributes
                tmp_dataset.images[original_index].types = updated_types
            # Add new image
            else:
                # Check that image.path is relative to zarr_dir
                if not image.path.startswith(tmp_dataset.zarr_dir):
                    raise ValueError(
                        f"{tmp_dataset.zarr_dir} is not a parent directory of "
                        f"{image.path}"
                    )
                # Propagate attributes and types from `origin` (if any)
                updated_attributes = {}
                updated_types = {}
                if image.origin is not None:
                    image_search = find_image_by_path(
                        images=tmp_dataset.images,
                        path=image.origin,
                    )
                    if image_search is not None:
                        original_img = image_search["image"]
                        updated_attributes = copy(original_img.attributes)
                        updated_types = copy(original_img.types)
                # Update image attributes/types with task output and manifest
                updated_attributes.update(image.attributes)
                updated_types.update(image.types)
                updated_types.update(task.output_types)
                new_image = SingleImage(
                    path=image.path,
                    origin=image.origin,
                    attributes=updated_attributes,
                    types=updated_types,
                )
                # Add image into the dataset image list
                tmp_dataset.images.append(new_image)

        # Remove images from Dataset.images
        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image.path not in current_task_output.image_list_removals
        ]

        # Update Dataset.filters.attributes:
        # current + (task_output: not really, in current examples..)
        if current_task_output.filters is not None:
            tmp_dataset.filters.attributes.update(
                current_task_output.filters.attributes
            )

        # Update Dataset.filters.types: current + (task_output + task_manifest)
        debug(task)
        if wftask.is_legacy_task:
            types_from_manifest = {}
        else:
            types_from_manifest = task.output_types
        if current_task_output.filters is not None:
            types_from_task = current_task_output.filters.types
        else:
            types_from_task = {}
        # Check that key sets are disjoint
        set_types_from_manifest = set(types_from_manifest.keys())
        set_types_from_task = set(types_from_task.keys())
        if not set_types_from_manifest.isdisjoint(set_types_from_task):
            overlap = set_types_from_manifest.intersection(set_types_from_task)
            raise ValueError(
                "Both task and task manifest did set the same"
                f"output type. Overlapping keys: {overlap}."
            )
        # Update Dataset.filters.types
        debug(types_from_manifest)
        tmp_dataset.filters.types.update(types_from_manifest)
        tmp_dataset.filters.types.update(types_from_task)

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
