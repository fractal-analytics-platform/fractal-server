from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import Field

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from .images import ImageQuery
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import TaskType
from fractal_server.images.status_tools import enrich_images_unsorted_async
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from fractal_server.images.tools import aggregate_types
from fractal_server.images.tools import filter_image_list
from fractal_server.types import AttributeFilters

router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/verify-unique-types/",
    status_code=status.HTTP_200_OK,
)
async def verify_unique_types(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    query: ImageQuery | None = None,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[str]:
    # Get dataset
    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]

    # Filter images
    if query is None:
        filtered_images = dataset.images
    else:
        if IMAGE_STATUS_KEY in query.attribute_filters.keys():
            images = await enrich_images_unsorted_async(
                dataset_id=dataset_id,
                workflowtask_id=workflowtask_id,
                images=dataset.images,
                db=db,
            )
        else:
            images = dataset.images
        filtered_images = filter_image_list(
            images=images,
            attribute_filters=query.attribute_filters,
            type_filters=query.type_filters,
        )

    # Get actual values for each available type
    available_types = aggregate_types(filtered_images)
    values_per_type: dict[str, set] = {
        _type: set() for _type in available_types
    }
    for _img in filtered_images:
        for _type in available_types:
            values_per_type[_type].add(_img["types"].get(_type, False))

    # Find types with non-unique value
    non_unique_types = [
        key for key, value in values_per_type.items() if len(value) > 1
    ]
    non_unique_types = sorted(non_unique_types)

    return non_unique_types


class NonProcessedImagesPayload(BaseModel):
    attribute_filters: AttributeFilters = Field(default_factory=dict)
    type_filters: dict[str, bool] = Field(default_factory=dict)


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/non-processed/",
    status_code=status.HTTP_200_OK,
)
async def check_non_processed_images(
    project_id: int,
    dataset_id: int,
    workflow_id: int,
    workflowtask_id: int,
    filters: NonProcessedImagesPayload,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_task_id=workflowtask_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    if db_workflow_task.order == 0:
        # Skip check for first task in the workflow
        return JSONResponse(status_code=200, content=[])

    previous_wft = db_workflow.task_list[db_workflow_task.order - 1]

    if previous_wft.task.output_types != {}:
        # Skip check if previous task has non-trivial `output_types`
        return JSONResponse(status_code=200, content=[])
    elif previous_wft.task.type in [
        TaskType.CONVERTER_COMPOUND,
        TaskType.CONVERTER_NON_PARALLEL,
    ]:
        # Skip check if previous task is converter
        return JSONResponse(status_code=200, content=[])

    res = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = res["dataset"]
    filtered_images = filter_image_list(
        images=dataset.images,
        type_filters=filters.type_filters,
        attribute_filters=filters.attribute_filters,
    )

    filtered_images_with_status = await enrich_images_unsorted_async(
        dataset_id=dataset_id,
        workflowtask_id=previous_wft.id,
        images=filtered_images,
        db=db,
    )
    missing_zarr_urls = [
        img["zarr_url"]
        for img in filtered_images_with_status
        if img["attributes"][IMAGE_STATUS_KEY] != HistoryUnitStatus.DONE
    ]

    return JSONResponse(status_code=200, content=missing_zarr_urls)
