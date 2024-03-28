from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Callable
from typing import Optional

from ....images import Filters
from ....images import SingleImage
from ....images.tools import _filter_image_list
from ....images.tools import find_image_by_path
from ....images.tools import match_filter
from .runner_functions import no_op_submit_setup_call
from .runner_functions import run_v1_task_parallel
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import WorkflowTaskV2

# FIXME: define RESERVED_ARGUMENTS = [", ...]


def execute_tasks_v2(
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    executor: ThreadPoolExecutor,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    logger_name: Optional[str] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
) -> DatasetV2:

    if not workflow_dir.exists():
        workflow_dir.mkdir()

    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:
        task = wftask.task

        # PRE TASK EXECUTION

        # Get filtered images
        pre_type_filters = copy(dataset.filters["types"])
        pre_type_filters.update(wftask.input_filters["types"])
        pre_attribute_filters = copy(dataset.filters["attributes"])
        pre_attribute_filters.update(wftask.input_filters["attributes"])
        filtered_images = _filter_image_list(
            images=tmp_dataset.images,
            filters=Filters(
                types=pre_type_filters,
                attributes=pre_attribute_filters,
            ),
        )
        # Verify that filtered images comply with output types
        for image in filtered_images:
            if not match_filter(image, Filters(types=task.input_types)):
                raise ValueError(
                    f"Filtered images include {image.dict()}, which does "
                    f"not comply with {task.input_types=}."
                )

        # TASK EXECUTION (V2 or V1)
        if not wftask.is_legacy_task:
            if task.type == "non_parallel":
                current_task_output = run_v2_task_non_parallel(
                    images=filtered_images,
                    zarr_dir=tmp_dataset.zarr_dir,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            elif task.type == "parallel":
                current_task_output = run_v2_task_parallel(
                    images=filtered_images,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            elif task.type == "compound":
                current_task_output = run_v2_task_compound(
                    images=filtered_images,
                    zarr_dir=tmp_dataset.zarr_dir,
                    wftask=wftask,
                    task=wftask.task,
                    workflow_dir=workflow_dir,
                    workflow_dir_user=workflow_dir_user,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            else:
                raise ValueError(f"Invalid {task.type=}.")

        else:

            current_task_output = run_v1_task_parallel(
                images=filtered_images,
                wftask=wftask,
                task_legacy=wftask.task_legacy,
                executor=executor,
                logger_name=logger_name,
                submit_setup_call=submit_setup_call,
            )

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
        for image in current_task_output.image_list_removals:
            image_search = find_image_by_path(
                images=tmp_dataset.images, path=image["path"]
            )
            if image_search["index"] is None:
                raise
            else:
                tmp_dataset.images.pop(image_search["index"])

        # Update Dataset.filters.attributes:
        # current + (task_output: not really, in current examples..)
        if current_task_output.filters is not None:
            tmp_dataset.filters["attributes"].update(
                current_task_output.filters.attributes
            )

        # Update Dataset.filters.types: current + (task_output + task_manifest)
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
        tmp_dataset.filters["types"].update(types_from_manifest)
        tmp_dataset.filters["types"].update(types_from_task)

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

    return tmp_dataset
