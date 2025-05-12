from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import Field
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import delete

from ._aux_functions import _get_dataset_check_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.images import SingleImage
from fractal_server.images import SingleImageUpdate
from fractal_server.images.tools import aggregate_attributes
from fractal_server.images.tools import aggregate_types
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter
from fractal_server.types import AttributeFilters
from fractal_server.types import TypeFilters

router = APIRouter()


class ImagePage(PaginationResponse[SingleImage]):

    attributes: dict[str, list[Any]]
    types: list[str]


class ImageQuery(BaseModel):
    type_filters: TypeFilters = Field(default_factory=dict)
    attribute_filters: AttributeFilters = Field(default_factory=dict)


class ImageQueryWithZarrUrl(ImageQuery):
    zarr_url: str | None = None


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_201_CREATED,
)
async def post_new_image(
    project_id: int,
    dataset_id: int,
    new_image: SingleImage,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]

    if not new_image.zarr_url.startswith(dataset.zarr_dir):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot create image with zarr_url which is not relative to "
                f"{dataset.zarr_dir}."
            ),
        )
    elif new_image.zarr_url == dataset.zarr_dir:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "`SingleImage.zarr_url` cannot be equal to `Dataset.zarr_dir`:"
                f" {dataset.zarr_dir}"
            ),
        )

    if new_image.zarr_url in dataset.image_zarr_urls:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Image with zarr_url '{new_image.zarr_url}' "
                f"already in DatasetV2 {dataset_id}",
            ),
        )

    dataset.images.append(new_image.model_dump())
    flag_modified(dataset, "images")

    await db.commit()

    return Response(status_code=status.HTTP_201_CREATED)


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/query/",
    response_model=ImagePage,
    status_code=status.HTTP_200_OK,
)
async def query_dataset_images(
    project_id: int,
    dataset_id: int,
    query: ImageQueryWithZarrUrl | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ImagePage:

    page = pagination.page
    page_size = pagination.page_size

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]
    images = dataset.images

    attributes = aggregate_attributes(images)
    types = aggregate_types(images)

    if query is not None:

        if query.zarr_url is not None:
            image = next(
                (
                    image
                    for image in images
                    if image["zarr_url"] == query.zarr_url
                ),
                None,
            )
            if image is None:
                images = []
            else:
                images = [image]

        if query.attribute_filters or query.type_filters:
            images = [
                image
                for image in images
                if match_filter(
                    image=image,
                    type_filters=query.type_filters,
                    attribute_filters=query.attribute_filters,
                )
            ]

    total_count = len(images)

    if page_size is None:
        page_size = total_count

    if total_count > 0:
        last_page = (total_count // page_size) + (total_count % page_size > 0)
        if page > last_page:
            page = last_page
        offset = (page - 1) * page_size
        images = images[offset : offset + page_size]

    return ImagePage(
        total_count=total_count,
        current_page=page,
        page_size=page_size,
        items=images,
        attributes=attributes,
        types=types,
    )


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dataset_images(
    project_id: int,
    dataset_id: int,
    zarr_url: str,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]

    image_to_remove = find_image_by_zarr_url(
        images=dataset.images, zarr_url=zarr_url
    )

    if image_to_remove is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No image with zarr_url '{zarr_url}' in "
                f"DatasetV2 {dataset_id}."
            ),
        )

    dataset.images.remove(image_to_remove["image"])
    flag_modified(dataset, "images")

    await db.execute(
        delete(HistoryImageCache)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.zarr_url == zarr_url)
    )

    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    response_model=SingleImage,
    status_code=status.HTTP_200_OK,
)
async def patch_dataset_image(
    project_id: int,
    dataset_id: int,
    image_update: SingleImageUpdate,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    db_dataset = output["dataset"]

    ret = find_image_by_zarr_url(
        images=db_dataset.images, zarr_url=image_update.zarr_url
    )
    if ret is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No image with zarr_url '{image_update.zarr_url}' in "
                f"DatasetV2 {dataset_id}."
            ),
        )
    index = ret["index"]

    for key, value in image_update.model_dump(
        exclude_none=True, exclude={"zarr_url"}
    ).items():
        db_dataset.images[index][key] = value

    flag_modified(db_dataset, "images")

    await db.commit()
    await db.close()
    return db_dataset.images[index]
