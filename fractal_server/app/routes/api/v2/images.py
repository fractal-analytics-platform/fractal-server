from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import validator
from sqlalchemy.orm.attributes import flag_modified

from ._aux_functions import _get_dataset_check_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.security import current_active_user
from fractal_server.app.security import User
from fractal_server.images import SingleImage
from fractal_server.images import val_scalar_dict

router = APIRouter()


class ImagePage(BaseModel):

    total_count: int
    page_size: int
    current_page: int

    attributes: list[str]
    images: list[SingleImage]


class ImageQuery(BaseModel):
    path: Optional[str]
    attributes: Optional[dict[str, Any]]

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/query/",
    response_model=ImagePage,
    status_code=status.HTTP_200_OK,
)
async def query_dataset_images(
    project_id: int,
    dataset_id: int,
    page: int = 1,  # query param
    page_size: Optional[int] = None,  # query param
    query: Optional[ImageQuery] = None,  # body
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ImagePage:

    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid pagination parameters: page={page} < 1",
        )

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    images = output["dataset"].images
    attributes = list(
        set(
            [
                attribute
                for image in images
                for attribute in image["attributes"].keys()
            ]
        )
    )

    if query is not None:

        if query.path is not None:
            image = next(
                (image for image in images if image["path"] == query.path),
                None,
            )
            if image is None:
                images = []
            else:
                images = [image]

        if query.attributes is not None:
            images = [
                image
                for image in images
                if SingleImage(**image).match_filter(query.attributes)
            ]

    total_count = len(images)
    if page_size is not None:
        if page_size < 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Invalid pagination parameters: page_size={page_size} < 0"
                ),
            )
        offset = (page - 1) * page_size
        images = images[offset : offset + page_size]  # noqa E203
    else:
        page_size = total_count

    return ImagePage(
        total_count=total_count,
        current_page=page,
        page_size=page_size,
        attributes=attributes,
        images=images,
    )


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dataset_images(
    project_id: int,
    dataset_id: int,
    path: str,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]

    image_to_remove = next(
        (image for image in dataset.images if image["path"] == path), None
    )
    if image_to_remove is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No image with path '{path}' in DatasetV2 {dataset_id}.",
        )

    dataset.images.remove(image_to_remove)
    flag_modified(dataset, "images")

    await db.merge(dataset)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
