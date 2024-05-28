import json
import logging
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Callable
from typing import Optional

from ....images import Filters
from ....images import SingleImage
from ....images.tools import filter_image_list
from ....images.tools import find_image_by_zarr_url
from ....images.tools import match_filter
from ..exceptions import JobExecutionError
from ..filenames import FILTERS_FILENAME
from ..filenames import HISTORY_FILENAME
from ..filenames import IMAGES_FILENAME
from .runner_functions import no_op_submit_setup_call
from .runner_functions import run_v1_task_parallel
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from .task_interface import TaskOutput
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.schemas.v2.dataset import _DatasetHistoryItemV2
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskStatusTypeV2


def execute_tasks_v2(
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    executor: ThreadPoolExecutor,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
) -> DatasetV2:

    logger = logging.getLogger(logger_name)

    if (
        not workflow_dir_local.exists()
    ):  # FIXME: this should have already happened
        workflow_dir_local.mkdir()

    # Initialize local dataset attributes
    zarr_dir = dataset.zarr_dir
    tmp_images = deepcopy(dataset.images)
    tmp_filters = deepcopy(dataset.filters)
    tmp_history = []

    for wftask in wf_task_list:
        task = wftask.task
        task_legacy = wftask.task_legacy
        if wftask.is_legacy_task:
            task_name = task_legacy.name
            logger.debug(
                f"SUBMIT {wftask.order}-th task "
                f'(legacy, name="{task_name}")'
            )
        else:
            task_name = task.name
            logger.debug(f'SUBMIT {wftask.order}-th task (name="{task_name}")')

        # PRE TASK EXECUTION

        # Get filtered images
        pre_filters = dict(
            types=copy(tmp_filters["types"]),
            attributes=copy(tmp_filters["attributes"]),
        )
        pre_filters["types"].update(wftask.input_filters["types"])
        pre_filters["attributes"].update(wftask.input_filters["attributes"])
        filtered_images = filter_image_list(
            images=tmp_images,
            filters=Filters(**pre_filters),
        )
        # Verify that filtered images comply with task input_types
        if not wftask.is_legacy_task:
            for image in filtered_images:
                if not match_filter(image, Filters(types=task.input_types)):
                    raise JobExecutionError(
                        "Invalid filtered image list\n"
                        f"Task input types: {task.input_types=}\n"
                        f'Image zarr_url: {image["zarr_url"]}\n'
                        f'Image types: {image["types"]}\n'
                    )

        # TASK EXECUTION (V2)
        if not wftask.is_legacy_task:
            if task.type == "non_parallel":
                current_task_output = run_v2_task_non_parallel(
                    images=filtered_images,
                    zarr_dir=zarr_dir,
                    wftask=wftask,
                    task=task,
                    workflow_dir_local=workflow_dir_local,
                    workflow_dir_remote=workflow_dir_remote,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            elif task.type == "parallel":
                current_task_output = run_v2_task_parallel(
                    images=filtered_images,
                    wftask=wftask,
                    task=task,
                    workflow_dir_local=workflow_dir_local,
                    workflow_dir_remote=workflow_dir_remote,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            elif task.type == "compound":
                current_task_output = run_v2_task_compound(
                    images=filtered_images,
                    zarr_dir=zarr_dir,
                    wftask=wftask,
                    task=task,
                    workflow_dir_local=workflow_dir_local,
                    workflow_dir_remote=workflow_dir_remote,
                    executor=executor,
                    logger_name=logger_name,
                    submit_setup_call=submit_setup_call,
                )
            else:
                raise ValueError(f"Unexpected error: Invalid {task.type=}.")
        # TASK EXECUTION (V1)
        else:
            current_task_output = run_v1_task_parallel(
                images=filtered_images,
                wftask=wftask,
                task_legacy=task_legacy,
                executor=executor,
                logger_name=logger_name,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                submit_setup_call=submit_setup_call,
            )

        # POST TASK EXECUTION

        # If `current_task_output` includes no images (to be created, edited or
        # removed), then flag all the input images as modified. See
        # fractal-server issue #1374.
        if (
            current_task_output.image_list_updates == []
            and current_task_output.image_list_removals == []
        ):
            current_task_output = TaskOutput(
                **current_task_output.dict(exclude={"image_list_updates"}),
                image_list_updates=[
                    dict(zarr_url=img["zarr_url"]) for img in filtered_images
                ],
            )

        # Update image list
        current_task_output.check_zarr_urls_are_unique()
        for image_obj in current_task_output.image_list_updates:
            image = image_obj.dict()
            # Edit existing image
            tmp_image_paths = [img["zarr_url"] for img in tmp_images]
            if image["zarr_url"] in tmp_image_paths:
                if (
                    image["origin"] is not None
                    and image["origin"] != image["zarr_url"]
                ):
                    raise JobExecutionError(
                        "Cannot edit an image with zarr_url different from "
                        "origin.\n"
                        f"zarr_url={image['zarr_url']}\n"
                        f"origin={image['origin']}"
                    )
                img_search = find_image_by_zarr_url(
                    images=tmp_images,
                    zarr_url=image["zarr_url"],
                )
                if img_search is None:
                    raise ValueError(
                        "Unexpected error: "
                        f"Image with zarr_url {image['zarr_url']} not found, "
                        "while updating image list."
                    )
                original_img = img_search["image"]
                original_index = img_search["index"]
                updated_attributes = copy(original_img["attributes"])
                updated_types = copy(original_img["types"])

                # Update image attributes/types with task output and manifest
                updated_attributes.update(image["attributes"])
                updated_types.update(image["types"])
                if not wftask.is_legacy_task:
                    updated_types.update(task.output_types)

                # Unset attributes with None value
                updated_attributes = {
                    key: value
                    for key, value in updated_attributes.items()
                    if value is not None
                }

                # Validate new image
                SingleImage(
                    zarr_url=image["zarr_url"],
                    types=updated_types,
                    attributes=updated_attributes,
                )

                # Update image in the dataset image list
                tmp_images[original_index]["attributes"] = updated_attributes
                tmp_images[original_index]["types"] = updated_types
            # Add new image
            else:
                # Check that image['zarr_url'] is relative to zarr_dir
                if not image["zarr_url"].startswith(zarr_dir):
                    raise JobExecutionError(
                        "Cannot create image if zarr_dir is not a parent "
                        "directory of zarr_url.\n"
                        f"zarr_dir: {zarr_dir}\n"
                        f"zarr_url: {image['zarr_url']}"
                    )
                # Check that image['zarr_url'] is not equal to zarr_dir
                if image["zarr_url"] == zarr_dir:
                    raise JobExecutionError(
                        "Cannot create image if zarr_url is equal to "
                        "zarr_dir.\n"
                        f"zarr_dir: {zarr_dir}\n"
                        f"zarr_url: {image['zarr_url']}"
                    )
                # Propagate attributes and types from `origin` (if any)
                updated_attributes = {}
                updated_types = {}
                if image["origin"] is not None:
                    img_search = find_image_by_zarr_url(
                        images=tmp_images,
                        zarr_url=image["origin"],
                    )
                    if img_search is not None:
                        original_img = img_search["image"]
                        updated_attributes = copy(original_img["attributes"])
                        updated_types = copy(original_img["types"])
                # Update image attributes/types with task output and manifest
                updated_attributes.update(image["attributes"])
                updated_attributes = {
                    key: value
                    for key, value in updated_attributes.items()
                    if value is not None
                }
                updated_types.update(image["types"])
                if not wftask.is_legacy_task:
                    updated_types.update(task.output_types)
                new_image = dict(
                    zarr_url=image["zarr_url"],
                    origin=image["origin"],
                    attributes=updated_attributes,
                    types=updated_types,
                )
                # Validate new image
                SingleImage(**new_image)
                # Add image into the dataset image list
                tmp_images.append(new_image)

        # Remove images from tmp_images
        for img_zarr_url in current_task_output.image_list_removals:
            img_search = find_image_by_zarr_url(
                images=tmp_images, zarr_url=img_zarr_url
            )
            if img_search is None:
                raise JobExecutionError(
                    f"Cannot remove missing image (zarr_url={img_zarr_url})."
                )
            else:
                tmp_images.pop(img_search["index"])

        # Update filters.attributes:
        # current + (task_output: not really, in current examples..)
        if current_task_output.filters is not None:
            tmp_filters["attributes"].update(
                current_task_output.filters.attributes
            )

        # Find manifest ouptut types
        if wftask.is_legacy_task:
            types_from_manifest = {}
        else:
            types_from_manifest = task.output_types

        # Find task-output types
        if current_task_output.filters is not None:
            types_from_task = current_task_output.filters.types
        else:
            types_from_task = {}

        # Check that key sets are disjoint
        set_types_from_manifest = set(types_from_manifest.keys())
        set_types_from_task = set(types_from_task.keys())
        if not set_types_from_manifest.isdisjoint(set_types_from_task):
            overlap = set_types_from_manifest.intersection(set_types_from_task)
            raise JobExecutionError(
                "Some type filters are being set twice, "
                f"for task '{task_name}'.\n"
                f"Types from task output: {types_from_task}\n"
                f"Types from task maniest: {types_from_manifest}\n"
                f"Overlapping keys: {overlap}"
            )

        # Update filters.types
        tmp_filters["types"].update(types_from_manifest)
        tmp_filters["types"].update(types_from_task)

        # Update history (based on _DatasetHistoryItemV2)
        history_item = _DatasetHistoryItemV2(
            workflowtask=wftask,
            status=WorkflowTaskStatusTypeV2.DONE,
            parallelization=dict(
                # task_type=wftask.task.type,  # FIXME: breaks for V1 tasks
                # component_list=fil, #FIXME
            ),
        ).dict()
        tmp_history.append(history_item)

        # Write current dataset attributes (history, images, filters) into
        # temporary files which can be used (1) to retrieve the latest state
        # when the job fails, (2) from within endpoints that need up-to-date
        # information
        with open(workflow_dir_local / HISTORY_FILENAME, "w") as f:
            json.dump(tmp_history, f, indent=2)
        with open(workflow_dir_local / FILTERS_FILENAME, "w") as f:
            json.dump(tmp_filters, f, indent=2)
        with open(workflow_dir_local / IMAGES_FILENAME, "w") as f:
            json.dump(tmp_images, f, indent=2)

        logger.debug(f'END    {wftask.order}-th task (name="{task_name}")')

    # NOTE: tmp_history only contains the newly-added history items (to be
    # appended to the original history), while tmp_filters and tmp_images
    # represent the new attributes (to replace the original ones)
    result = dict(
        history=tmp_history,
        filters=tmp_filters,
        images=tmp_images,
    )
    return result
