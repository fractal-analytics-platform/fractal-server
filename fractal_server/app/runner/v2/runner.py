import logging
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Callable
from typing import Optional

from sqlalchemy.orm.attributes import flag_modified

from ....images import SingleImage
from ....images.tools import filter_image_list
from ....images.tools import find_image_by_zarr_url
from ..exceptions import JobExecutionError
from .runner_functions import no_op_submit_setup_call
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from .task_interface import TaskOutput
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.schemas.v2.dataset import _DatasetHistoryItemV2
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskStatusTypeV2
from fractal_server.images.models import AttributeFiltersType
from fractal_server.images.tools import merge_type_filters


def execute_tasks_v2(
    *,
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    executor: ThreadPoolExecutor,
    user_id: int,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
    submit_setup_call: Callable = no_op_submit_setup_call,
    job_attribute_filters: AttributeFiltersType,
) -> None:
    logger = logging.getLogger(logger_name)

    if not workflow_dir_local.exists():
        logger.warning(
            f"Now creating {workflow_dir_local}, "
            "but it should have already happened."
        )
        workflow_dir_local.mkdir()

    # Initialize local dataset attributes
    zarr_dir = dataset.zarr_dir
    tmp_images = deepcopy(dataset.images)
    current_dataset_type_filters = deepcopy(dataset.type_filters)

    for wftask in wf_task_list:
        task = wftask.task
        task_name = task.name
        logger.debug(f'SUBMIT {wftask.order}-th task (name="{task_name}")')

        # PRE TASK EXECUTION

        # Get filtered images
        type_filters = copy(current_dataset_type_filters)
        type_filters_patch = merge_type_filters(
            task_input_types=task.input_types,
            wftask_type_filters=wftask.type_filters,
        )
        type_filters.update(type_filters_patch)
        filtered_images = filter_image_list(
            images=tmp_images,
            type_filters=type_filters,
            attribute_filters=job_attribute_filters,
        )

        # First, set status SUBMITTED in dataset.history for each wftask
        with next(get_sync_db()) as db:
            db_dataset = db.get(DatasetV2, dataset.id)
            new_history_item = _DatasetHistoryItemV2(
                workflowtask=dict(
                    **wftask.model_dump(exclude={"task"}),
                    task=wftask.task.model_dump(),
                ),
                status=WorkflowTaskStatusTypeV2.SUBMITTED,
                parallelization=dict(),  # FIXME: re-include parallelization
            ).model_dump()
            db_dataset.history.append(new_history_item)
            flag_modified(db_dataset, "history")
            db.merge(db_dataset)
            db.commit()
        # TASK EXECUTION (V2)
        if task.type == "non_parallel":
            current_task_output, num_tasks = run_v2_task_non_parallel(
                images=filtered_images,
                zarr_dir=zarr_dir,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                executor=executor,
                submit_setup_call=submit_setup_call,
            )
        elif task.type == "parallel":
            current_task_output, num_tasks = run_v2_task_parallel(
                images=filtered_images,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                executor=executor,
                submit_setup_call=submit_setup_call,
            )
        elif task.type == "compound":
            current_task_output, num_tasks = run_v2_task_compound(
                images=filtered_images,
                zarr_dir=zarr_dir,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                executor=executor,
                submit_setup_call=submit_setup_call,
            )
        else:
            raise ValueError(f"Unexpected error: Invalid {task.type=}.")

        # POST TASK EXECUTION

        # If `current_task_output` includes no images (to be created, edited or
        # removed), then flag all the input images as modified. See
        # fractal-server issue #1374.
        if (
            current_task_output.image_list_updates == []
            and current_task_output.image_list_removals == []
        ):
            current_task_output = TaskOutput(
                **current_task_output.model_dump(
                    exclude={"image_list_updates"}
                ),
                image_list_updates=[
                    dict(zarr_url=img["zarr_url"]) for img in filtered_images
                ],
            )

        # Update image list
        num_new_images = 0
        current_task_output.check_zarr_urls_are_unique()
        for image_obj in current_task_output.image_list_updates:
            image = image_obj.model_dump()
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
                num_new_images += 1

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

        # Update type_filters based on task-manifest output_types
        type_filters_from_task_manifest = task.output_types
        current_dataset_type_filters.update(type_filters_from_task_manifest)

        # Write current dataset attributes (history, images, filters) into the
        # database. They can be used (1) to retrieve the latest state
        # when the job fails, (2) from within endpoints that need up-to-date
        # information
        with next(get_sync_db()) as db:
            db_dataset = db.get(DatasetV2, dataset.id)
            db_dataset.history[-1]["status"] = WorkflowTaskStatusTypeV2.DONE
            db_dataset.type_filters = current_dataset_type_filters
            db_dataset.images = tmp_images
            for attribute_name in [
                "type_filters",
                "history",
                "images",
            ]:
                flag_modified(db_dataset, attribute_name)
            db.merge(db_dataset)
            db.commit()

            # Create accounting record
            record = AccountingRecord(
                user_id=user_id,
                num_tasks=num_tasks,
                num_new_images=num_new_images,
            )
            db.add(record)
            db.commit()

        logger.debug(f'END    {wftask.order}-th task (name="{task_name}")')
