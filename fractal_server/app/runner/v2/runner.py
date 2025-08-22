import logging
from collections.abc import Callable
from copy import copy
from copy import deepcopy
from pathlib import Path
from typing import Any
from typing import Literal

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import delete
from sqlmodel import update

from .merge_outputs import merge_outputs
from .runner_functions import run_v2_task_compound
from .runner_functions import run_v2_task_non_parallel
from .runner_functions import run_v2_task_parallel
from .runner_functions import SubmissionOutcome
from .task_interface import TaskOutput
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.v2.db_tools import update_status_of_history_run
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import TaskDumpV2
from fractal_server.app.schemas.v2 import TaskGroupDumpV2
from fractal_server.app.schemas.v2 import TaskType
from fractal_server.images import SingleImage
from fractal_server.images.status_tools import enrich_images_unsorted_sync
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from fractal_server.images.tools import filter_image_list
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import merge_type_filters
from fractal_server.types import AttributeFilters


def _remove_status_from_attributes(
    images: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Drop attribute `IMAGE_STATUS_KEY` from all images.
    """
    images_copy = deepcopy(images)
    [img["attributes"].pop(IMAGE_STATUS_KEY, None) for img in images_copy]
    return images_copy


def drop_none_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
    # Unset attributes with `None` value
    non_none_attributes = {
        key: value for key, value in attributes.items() if value is not None
    }
    return non_none_attributes


def get_origin_attribute_and_types(
    *,
    origin_url: str,
    images: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, bool]]:
    """
    Search for origin image and extract its attributes/types.
    """
    origin_img_search = find_image_by_zarr_url(
        images=images,
        zarr_url=origin_url,
    )
    if origin_img_search is None:
        updated_attributes = {}
        updated_types = {}
    else:
        origin_image = origin_img_search["image"]
        updated_attributes = copy(origin_image["attributes"])
        updated_types = copy(origin_image["types"])
    return updated_attributes, updated_types


def execute_tasks_v2(
    *,
    wf_task_list: list[WorkflowTaskV2],
    dataset: DatasetV2,
    runner: BaseRunner,
    user_id: int,
    workflow_dir_local: Path,
    job_id: int,
    workflow_dir_remote: Path | None = None,
    logger_name: str | None = None,
    get_runner_config: Callable[
        [
            WorkflowTaskV2,
            Literal["non_parallel", "parallel"],
            Path | None,
        ],
        Any,
    ],
    job_type_filters: dict[str, bool],
    job_attribute_filters: AttributeFilters,
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

    ENRICH_IMAGES_WITH_STATUS: bool = (
        IMAGE_STATUS_KEY in job_attribute_filters.keys()
    )

    for ind_wftask, wftask in enumerate(wf_task_list):
        task = wftask.task
        task_name = task.name
        logger.debug(f'SUBMIT {wftask.order}-th task (name="{task_name}")')

        # PRE TASK EXECUTION

        # Filter images by types and attributes (in two steps)
        if wftask.task_type in [
            TaskType.COMPOUND,
            TaskType.PARALLEL,
            TaskType.NON_PARALLEL,
        ]:
            # Non-converter task
            type_filters = copy(current_dataset_type_filters)
            type_filters_patch = merge_type_filters(
                task_input_types=task.input_types,
                wftask_type_filters=wftask.type_filters,
            )
            type_filters.update(type_filters_patch)

            if ind_wftask == 0 and ENRICH_IMAGES_WITH_STATUS:
                # FIXME: Could this be done on `type_filtered_images`?
                tmp_images = enrich_images_unsorted_sync(
                    images=tmp_images,
                    dataset_id=dataset.id,
                    workflowtask_id=wftask.id,
                )
            type_filtered_images = filter_image_list(
                images=tmp_images,
                type_filters=type_filters,
            )
            num_available_images = len(type_filtered_images)

            filtered_images = filter_image_list(
                images=type_filtered_images,
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
                job_id=job_id,
                task_id=wftask.task.id,
                workflowtask_dump=workflowtask_dump,
                task_group_dump=task_group_dump,
                num_available_images=num_available_images,
                status=HistoryUnitStatus.SUBMITTED,
            )
            db.add(history_run)
            db.commit()
            db.refresh(history_run)
            history_run_id = history_run.id

        # TASK EXECUTION (V2)
        try:
            if task.type in [
                TaskType.NON_PARALLEL,
                TaskType.CONVERTER_NON_PARALLEL,
            ]:
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
                    user_id=user_id,
                    task_type=task.type,
                )
            elif task.type == TaskType.PARALLEL:
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
                    user_id=user_id,
                )
            elif task.type in [
                TaskType.COMPOUND,
                TaskType.CONVERTER_COMPOUND,
            ]:
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
                    user_id=user_id,
                )
            else:
                raise ValueError(f"Unexpected error: Invalid {task.type=}.")
        except Exception as e:
            outcomes_dict = {
                0: SubmissionOutcome(
                    result=None,
                    exception=e,
                )
            }
            num_tasks = 0

        # POST TASK EXECUTION
        try:
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
            # NOTE: In principle we could make the task-output processing more
            # granular, and also associate output-processing failures to
            # history status.
            for image_obj in current_task_output.image_list_updates:
                image = image_obj.model_dump()
                if image["zarr_url"] in [
                    img["zarr_url"] for img in tmp_images
                ]:
                    img_search = find_image_by_zarr_url(
                        images=tmp_images,
                        zarr_url=image["zarr_url"],
                    )
                    if img_search is None:
                        raise ValueError(
                            "Unexpected error: "
                            f"Image with zarr_url {image['zarr_url']} not "
                            "found, while updating image list."
                        )
                    existing_image_index = img_search["index"]

                    if (
                        image["origin"] is None
                        or image["origin"] == image["zarr_url"]
                    ):
                        # CASE 1: Edit existing image
                        existing_image = img_search["image"]
                        new_attributes = copy(existing_image["attributes"])
                        new_types = copy(existing_image["types"])
                        new_image = dict(
                            zarr_url=image["zarr_url"],
                        )
                        if "origin" in existing_image.keys():
                            new_image["origin"] = existing_image["origin"]
                    else:
                        # CASE 2: Re-create existing image based on `origin`
                        # Propagate attributes and types from `origin` (if any)
                        (
                            new_attributes,
                            new_types,
                        ) = get_origin_attribute_and_types(
                            origin_url=image["origin"],
                            images=tmp_images,
                        )
                        new_image = dict(
                            zarr_url=image["zarr_url"],
                            origin=image["origin"],
                        )
                    # Update attributes
                    new_attributes.update(image["attributes"])
                    new_attributes = drop_none_attributes(new_attributes)
                    new_image["attributes"] = new_attributes
                    # Update types
                    new_types.update(image["types"])
                    new_types.update(task.output_types)
                    new_image["types"] = new_types
                    # Validate new image
                    SingleImage(**new_image)
                    # Update image in the dataset image list
                    tmp_images[existing_image_index] = new_image

                else:
                    # CASE 3: Add new image
                    # Check that image['zarr_url'] is a subfolder of zarr_dir
                    if (
                        not image["zarr_url"].startswith(zarr_dir)
                        or image["zarr_url"] == zarr_dir
                    ):
                        raise JobExecutionError(
                            "Cannot create image if zarr_url is not a "
                            "subfolder of zarr_dir.\n"
                            f"zarr_dir: {zarr_dir}\n"
                            f"zarr_url: {image['zarr_url']}"
                        )

                    # Propagate attributes and types from `origin` (if any)
                    new_attributes, new_types = get_origin_attribute_and_types(
                        origin_url=image["origin"],
                        images=tmp_images,
                    )
                    # Prepare new image
                    new_attributes.update(image["attributes"])
                    new_attributes = drop_none_attributes(new_attributes)
                    new_types.update(image["types"])
                    new_types.update(task.output_types)
                    new_image = dict(
                        zarr_url=image["zarr_url"],
                        origin=image["origin"],
                        attributes=new_attributes,
                        types=new_types,
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
                        "Cannot remove missing image "
                        f"(zarr_url={img_zarr_url})."
                    )
                else:
                    tmp_images.pop(img_search["index"])

            # Update type_filters based on task-manifest output_types
            type_filters_from_task_manifest = task.output_types
            current_dataset_type_filters.update(
                type_filters_from_task_manifest
            )
        except Exception as e:
            logger.error(
                "Unexpected error in post-task-execution block. "
                f"Original error: {str(e)}"
            )
            with next(get_sync_db()) as db:
                db.execute(
                    update(HistoryUnit)
                    .where(HistoryUnit.history_run_id == history_run_id)
                    .values(status=HistoryUnitStatus.FAILED)
                )
                db.commit()
            raise e

        with next(get_sync_db()) as db:
            # Write current dataset images into the database.
            db_dataset = db.get(DatasetV2, dataset.id)
            if ENRICH_IMAGES_WITH_STATUS:
                db_dataset.images = _remove_status_from_attributes(tmp_images)
            else:
                db_dataset.images = tmp_images
            flag_modified(db_dataset, "images")
            db.merge(db_dataset)

            db.execute(
                delete(HistoryImageCache)
                .where(HistoryImageCache.dataset_id == dataset.id)
                .where(HistoryImageCache.workflowtask_id == wftask.id)
                .where(
                    HistoryImageCache.zarr_url.in_(
                        current_task_output.image_list_removals
                    )
                )
            )

            db.commit()
            db.close()  # NOTE: this is needed, but the reason is unclear

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
                logger.warning(
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
