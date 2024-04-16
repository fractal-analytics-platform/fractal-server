from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import Field
from sqlalchemy.orm.attributes import flag_modified

from ._aux_functions import _get_dataset_check_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.security import current_active_user
from fractal_server.app.security import User
from fractal_server.images import Filters
from fractal_server.images import SingleImage
from fractal_server.images import SingleImageUpdate
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter

router = APIRouter()


class ImagePage(BaseModel):

    total_count: int
    page_size: int
    current_page: int

    attributes: dict[str, list[Any]]
    types: list[str]

    images: list[SingleImage]


class ImageQuery(BaseModel):
    zarr_url: Optional[str]
    filters: Filters = Field(default_factory=Filters)


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_201_CREATED,
)
async def post_new_image(
    project_id: int,
    dataset_id: int,
    new_image: SingleImage,
    user: User = Depends(current_active_user),
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

    dataset.images.append(new_image.dict())
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
    use_dataset_filters: bool = False,  # query param
    page: int = 1,  # query param
    page_size: Optional[int] = None,  # query param
    query: Optional[ImageQuery] = None,  # body
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ImagePage:

    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid pagination parameter: page={page} < 1",
        )

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]
    images = dataset.images

    if use_dataset_filters is True:
        images = [
            image
            for image in images
            if match_filter(image, Filters(**dataset.filters))
        ]

    attributes = {}
    for image in images:
        for k, v in image["attributes"].items():
            attributes.setdefault(k, []).append(v)
        for k, v in attributes.items():
            attributes[k] = list(set(v))

    types = list(
        set(type for image in images for type in image["types"].keys())
    )

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

        if query.filters.attributes or query.filters.types:
            images = [
                image
                for image in images
                if match_filter(
                    image,
                    Filters(**query.filters.dict()),
                )
            ]

    total_count = len(images)

    if page_size is not None:
        if page_size <= 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Invalid pagination parameter: page_size={page_size} <= 0"
                ),
            )
    else:
        page_size = total_count

    if total_count == 0:
        page = 1
    else:
        last_page = (total_count // page_size) + (total_count % page_size > 0)
        if page > last_page:
            page = last_page
        offset = (page - 1) * page_size
        images = images[offset : offset + page_size]  # noqa E203

    return ImagePage(
        total_count=total_count,
        current_page=page,
        page_size=page_size,
        attributes=attributes,
        types=types,
        images=images,
    )


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dataset_images(
    project_id: int,
    dataset_id: int,
    zarr_url: str,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]

    image_to_remove = next(
        (image for image in dataset.images if image["zarr_url"] == zarr_url),
        None,
    )
    if image_to_remove is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No image with zarr_url '{zarr_url}' in "
                f"DatasetV2 {dataset_id}."
            ),
        )

    dataset.images.remove(image_to_remove)
    flag_modified(dataset, "images")

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
    user: User = Depends(current_active_user),
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

    for key, value in image_update.dict(
        exclude_none=True, exclude={"zarr_url"}
    ).items():
        db_dataset.images[index][key] = value

    flag_modified(db_dataset, "images")

    await db.commit()
    await db.close()
    return db_dataset.images[index]
