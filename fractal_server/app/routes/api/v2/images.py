import json
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Response
from fastapi import status
from pydantic import BaseModel

from ._aux_functions import _get_dataset_check_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.security import current_active_user
from fractal_server.app.security import User
from fractal_server.images import SingleImage
from fractal_server.images import val_scalar_dict


router = APIRouter()


class ImageCollection(BaseModel):
    images: list[SingleImage]
    attributes: list[str]


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    response_model=ImageCollection,
    status_code=status.HTTP_200_OK,
)
async def get_dataset_images(
    project_id: int,
    dataset_id: int,
    path: Optional[str] = None,
    attributes: Optional[str] = Query(
        None,
        description=(
            "String representation of a Python dictionary.<br><br>"
            "Curly braces are encoded as `%7B` and `%7D`.<br>"
            "Colomns become equal signs.<br>"
            "Keys and string values must be enclosed in double quotes, "
            "encoded as `%22`.<br>"
            "`None` becomes `null`,"
            "`True/False` becomes respectively `true/false`.<br>"
            "E.g. `{'a': 3, 'b': None, 'c': 'hello'}` must be encoded as "
            "`%7B%22a%22=3&%22b%22=null,%22c%22=%22hello%22%7D`.<br><br>"
            "Values must be of type `str`, `int`, `float`, `bool` or `None`."
        ),
    ),
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ImageCollection:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    images: list[SingleImage] = output["dataset"].images

    if path is not None:
        images = [image for image in images if image["path"] == path]

    if attributes is not None:
        try:
            attributes = json.loads(attributes)
            val_scalar_dict("")(attributes)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "The 'attributes' query parameter must be a valid dict. "
                    f"You provided: {attributes}"
                ),
            )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "The 'attributes' query parameter must be a scalar dict. "
                    f"You provided: {attributes}"
                ),
            )
        images = [
            image
            for image in images
            if SingleImage(**image).match_filter(attributes)
        ]

    return ImageCollection(
        images=images,
        attributes=list(
            set(
                [
                    attribute
                    for image in images
                    for attribute in image["attributes"].keys()
                ]
            )
        ),
    )


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dataset_images(
    project_id: int,
    dataset_id: int,
    path: list[str] = Query(...),
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )

    dataset: DatasetV2 = output["dataset"]

    if not all(p in dataset.image_paths for p in path):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"The list of paths you provided ({path}) is not a sublist of "
                f"{dataset.image_paths}."
            ),
        )

    new_images = [
        image for image in dataset.images if not image["path"] in path
    ]

    dataset.images = new_images

    await db.merge(dataset)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
)
async def patch_dataset_images(
    project_id: int,
    dataset_id: int,
    new_images: list[SingleImage],
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[SingleImage]:

    return Response(status_code=status.HTTP_501_NOT_IMPLEMENTED)
