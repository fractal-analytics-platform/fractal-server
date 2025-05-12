from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import Field
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from .images import ImageQuery
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import HistoryUnitStatus
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
        filtered_images = filter_image_list(
            images=dataset.images,
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
async def check_workflowtask(
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
        "converter_compound",
        "converter_non_parallel",
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

    filtered_zarr_urls = [image["zarr_url"] for image in filtered_images]

    res = await db.execute(
        select(HistoryImageCache.zarr_url)
        .join(HistoryUnit)
        .where(HistoryImageCache.zarr_url.in_(filtered_zarr_urls))
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == previous_wft.id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryUnit.status == HistoryUnitStatus.DONE)
    )
    done_zarr_urls = res.scalars().all()

    missing_zarr_urls = list(set(filtered_zarr_urls) - set(done_zarr_urls))

    return JSONResponse(status_code=200, content=missing_zarr_urls)
