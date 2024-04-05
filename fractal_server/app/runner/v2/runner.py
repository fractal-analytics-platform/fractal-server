import json
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Callable
from typing import Optional

from ....images import Filters
from ....images import SingleImage
from ....images.tools import filter_image_list
from ....images.tools import find_image_by_path
from ....images.tools import match_filter
from ..filenames import FILTERS_FILENAME
from ..filenames import HISTORY_FILENAME
from ..filenames import IMAGES_FILENAME
from .runner_functions import no_op_submit_setup_call
from .runner_functions import run_v1_task_parallel
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.schemas.v2.dataset import _DatasetHistoryItemV2
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskStatusTypeV2

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

    if not workflow_dir.exists():  # FIXME: this should have already happened
        workflow_dir.mkdir()

    # Initialize local dataset attributes
    zarr_dir = dataset.zarr_dir
    tmp_images = deepcopy(dataset.images)
    tmp_filters = deepcopy(dataset.filters)
    tmp_history = []

    for wftask in wf_task_list:
        task = wftask.task

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
        for image in filtered_images:
            if not match_filter(image, Filters(types=task.input_types)):
                raise ValueError(
                    f"Filtered images include {image.dict()}, which does "
                    f"not comply with {task.input_types=}."
                )

        # TASK EXECUTION (V2)
        if not wftask.is_legacy_task:
            if task.type == "non_parallel":
                current_task_output = run_v2_task_non_parallel(
                    images=filtered_images,
                    zarr_dir=zarr_dir,
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
                    zarr_dir=zarr_dir,
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
        # TASK EXECUTION (V1)
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
        for image_obj in current_task_output.image_list_updates:
            image = image_obj.dict()
            # Edit existing image
            if image["path"] in [_image["path"] for _image in tmp_images]:
                if (
                    image["origin"] is not None
                    and image["origin"] != image["path"]
                ):
                    raise ValueError(
                        f"Trying to edit an image with {image['path']=} "
                        f"and {image['origin']=}."
                    )
                image_search = find_image_by_path(
                    images=tmp_images,
                    path=image["path"],
                )
                if image_search is None:
                    raise ValueError(
                        f"Image with path {image['path']} not found, while "
                        "updating image list."
                    )
                original_img = image_search["image"]
                original_index = image_search["index"]
                updated_attributes = copy(original_img["attributes"])
                updated_types = copy(original_img["types"])

                # Update image attributes/types with task output and manifest
                updated_attributes.update(image["attributes"])
                updated_types.update(image["types"])
                updated_types.update(task.output_types)

                # Update image in the dataset image list
                tmp_images[original_index]["attributes"] = updated_attributes
                tmp_images[original_index]["types"] = updated_types
            # Add new image
            else:
                # Check that image['path'] is relative to zarr_dir
                if not image["path"].startswith(zarr_dir):
                    raise ValueError(
                        f"{zarr_dir} is not a parent directory of "
                        f"{image['path']}"
                    )
                # Propagate attributes and types from `origin` (if any)
                updated_attributes = {}
                updated_types = {}
                if image["origin"] is not None:
                    image_search = find_image_by_path(
                        images=tmp_images,
                        path=image["origin"],
                    )
                    if image_search is not None:
                        original_img = image_search["image"]
                        updated_attributes = copy(original_img["attributes"])
                        updated_types = copy(original_img["types"])
                # Update image attributes/types with task output and manifest
                updated_attributes.update(image["attributes"])
                updated_types.update(image["types"])
                updated_types.update(task.output_types)
                new_image = dict(
                    path=image["path"],
                    origin=image["origin"],
                    attributes=updated_attributes,
                    types=updated_types,
                )
                # Validate new image
                SingleImage(**new_image)
                # Add image into the dataset image list
                tmp_images.append(new_image)

        # Remove images from tmp_images
        for image_path in current_task_output.image_list_removals:
            image_search = find_image_by_path(
                images=tmp_images, path=image_path
            )
            if image_search is None:
                raise ValueError(
                    f"Cannot remove missing image with path {image_path=}"
                )
            else:
                tmp_images.pop(image_search["index"])

        # Update filters.attributes:
        # current + (task_output: not really, in current examples..)
        if current_task_output.filters is not None:
            tmp_filters["attributes"].update(
                current_task_output.filters.attributes
            )

        # Update filters.types: current + (task_output + task_manifest)
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
        with open(workflow_dir / HISTORY_FILENAME, "w") as f:
            json.dump(tmp_history, f, indent=2)
        with open(workflow_dir / FILTERS_FILENAME, "w") as f:
            json.dump(tmp_filters, f, indent=2)
        with open(workflow_dir / IMAGES_FILENAME, "w") as f:
            json.dump(tmp_images, f, indent=2)

    # NOTE: tmp_history only contains the newly-added history items (to be
    # appended to the original history), while tmp_filters and tmp_images
    # represent the new attributes (to replace the original ones)
    result = dict(
        history=tmp_history,
        filters=tmp_filters,
        images=tmp_images,
    )
    return result
