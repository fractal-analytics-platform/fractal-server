import logging
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

from sqlalchemy.orm.attributes import flag_modified

from ....images import SingleImage
from ....images.tools import filter_image_list
from ....images.tools import find_image_by_zarr_url
from ..exceptions import JobExecutionError
from .merge_outputs import merge_outputs
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from .task_interface import TaskOutput
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.v2.db_tools import update_status_of_history_run
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import TaskDumpV2
from fractal_server.app.schemas.v2 import TaskGroupDumpV2
from fractal_server.images.models import AttributeFiltersType
from fractal_server.images.tools import merge_type_filters


def execute_tasks_v2(
    *,
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    runner: BaseRunner,
    user_id: int,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    logger_name: Optional[str] = None,
    get_runner_config: Callable[
        [
            WorkflowTaskV2,
            Literal["non_parallel", "parallel"],
            Optional[Path],
        ],
        Any,
    ],
    job_type_filters: dict[str, bool],
    job_attribute_filters: AttributeFiltersType,
) -> None:
    logger = logging.getLogger(logger_name)

    if not workflow_dir_local.exists():
        logger.warning(
            f"Now creating {workflow_dir_local}, but it "
            "should have already happened."
        )
        workflow_dir_local.mkdir()

    # For local backend, remote and local folders are the same
    if workflow_dir_remote is None:
        workflow_dir_remote = workflow_dir_local

    # Initialize local dataset attributes
    zarr_dir = dataset.zarr_dir
    tmp_images = deepcopy(dataset.images)
    current_dataset_type_filters = copy(job_type_filters)

    for wftask in wf_task_list:
        task = wftask.task
        task_name = task.name
        logger.debug(f'SUBMIT {wftask.order}-th task (name="{task_name}")')

        # PRE TASK EXECUTION

        # Filter images by types and attributes (in two steps)
        if wftask.task_type in ["compound", "parallel", "non_parallel"]:
            # Non-converter task
            type_filters = copy(current_dataset_type_filters)
            type_filters_patch = merge_type_filters(
                task_input_types=task.input_types,
                wftask_type_filters=wftask.type_filters,
            )
            type_filters.update(type_filters_patch)
            type_filtered_images = filter_image_list(
                images=tmp_images,
                type_filters=type_filters,
                attribute_filters=None,
            )
            num_available_images = len(type_filtered_images)
            filtered_images = filter_image_list(
                images=type_filtered_images,
                type_filters=None,
                attribute_filters=job_attribute_filters,
            )
        else:
            # Converter task
            filtered_images = []
            num_available_images = 0

        with next(get_sync_db()) as db:
            # Create dumps for workflowtask and taskgroup
            workflowtask_dump = dict(
                **wftask.model_dump(exclude={"task"}),
                task=TaskDumpV2(**wftask.task.model_dump()).model_dump(),
            )
            task_group = db.get(TaskGroupV2, wftask.task.taskgroupv2_id)
            task_group_dump = TaskGroupDumpV2(
                **task_group.model_dump()
            ).model_dump()
            # Create HistoryRun
            history_run = HistoryRun(
                dataset_id=dataset.id,
                workflowtask_id=wftask.id,
                workflowtask_dump=workflowtask_dump,
                task_group_dump=task_group_dump,
                num_available_images=num_available_images,
                status=HistoryUnitStatus.SUBMITTED,
            )
            db.add(history_run)
            db.commit()
            db.refresh(history_run)
            history_run_id = history_run.id
            logger.debug(
                f"Created {history_run_id=}, for "
                f"{wftask.id=} and {dataset.id=}"
            )

        # TASK EXECUTION (V2)
        if task.type in ["non_parallel", "converter_non_parallel"]:
            outcomes_dict, num_tasks = run_v2_task_non_parallel(
                images=filtered_images,
                zarr_dir=zarr_dir,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                runner=runner,
                get_runner_config=get_runner_config,
                history_run_id=history_run_id,
                dataset_id=dataset.id,
                task_type=task.type,
            )
        elif task.type == "parallel":
            outcomes_dict, num_tasks = run_v2_task_parallel(
                images=filtered_images,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                runner=runner,
                get_runner_config=get_runner_config,
                history_run_id=history_run_id,
                dataset_id=dataset.id,
            )
        elif task.type in ["compound", "converter_compound"]:
            outcomes_dict, num_tasks = run_v2_task_compound(
                images=filtered_images,
                zarr_dir=zarr_dir,
                wftask=wftask,
                task=task,
                workflow_dir_local=workflow_dir_local,
                workflow_dir_remote=workflow_dir_remote,
                runner=runner,
                get_runner_config=get_runner_config,
                history_run_id=history_run_id,
                dataset_id=dataset.id,
                task_type=task.type,
            )
        else:
            raise ValueError(f"Unexpected error: Invalid {task.type=}.")

        # POST TASK EXECUTION

        non_failed_task_outputs = [
            value.task_output
            for value in outcomes_dict.values()
            if value.task_output is not None
        ]
        if len(non_failed_task_outputs) > 0:
            current_task_output = merge_outputs(non_failed_task_outputs)
            # If `current_task_output` includes no images (to be created or
            # removed), then flag all the input images as modified.
            # See fractal-server issues #1374 and #2409.
            if (
                current_task_output.image_list_updates == []
                and current_task_output.image_list_removals == []
            ):
                current_task_output = TaskOutput(
                    image_list_updates=[
                        dict(zarr_url=img["zarr_url"])
                        for img in filtered_images
                    ],
                )
        else:
            current_task_output = TaskOutput()

        # Update image list
        num_new_images = 0
        current_task_output.check_zarr_urls_are_unique()
        # FIXME: Introduce for loop over task outputs, and processe them
        # sequentially
        # each failure should lead to an update of the specific image status
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

        with next(get_sync_db()) as db:
            # Write current dataset images into the database.
            db_dataset = db.get(DatasetV2, dataset.id)
            db_dataset.images = tmp_images
            flag_modified(db_dataset, "images")
            db.merge(db_dataset)
            db.commit()
            db.close()  # FIXME: why is this needed?

            # Create accounting record
            record = AccountingRecord(
                user_id=user_id,
                num_tasks=num_tasks,
                num_new_images=num_new_images,
            )
            db.add(record)
            db.commit()

            # Update `HistoryRun` entry, and raise an error if task failed
            try:
                first_exception = next(
                    value.exception
                    for value in outcomes_dict.values()
                    if value.exception is not None
                )
                # An exception was found
                update_status_of_history_run(
                    history_run_id=history_run_id,
                    status=HistoryUnitStatus.FAILED,
                    db_sync=db,
                )
                logger.error(
                    f'END    {wftask.order}-th task (name="{task_name}") - '
                    "ERROR."
                )
                # Raise first error
                raise JobExecutionError(
                    info=(
                        f"An error occurred.\n"
                        f"Original error:\n{first_exception}"
                    )
                )
            except StopIteration:
                # No exception was found
                update_status_of_history_run(
                    history_run_id=history_run_id,
                    status=HistoryUnitStatus.DONE,
                    db_sync=db,
                )
                db.commit()
                logger.debug(
                    f'END    {wftask.order}-th task (name="{task_name}")'
                )
