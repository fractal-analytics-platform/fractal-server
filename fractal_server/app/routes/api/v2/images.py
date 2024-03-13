from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import status
from pydantic import BaseModel
from pydantic import validator

from .....images import SingleImage
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import DatasetV2
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_dataset_check_owner


router = APIRouter()


class ImageCollection(BaseModel):
    images: list[SingleImage]
    attributes: list[str]


class ImageQuery(BaseModel):
    path: Optional[str]
    attributes: Optional[dict[str, Any]]

    @validator("attributes")
    def cast_types(cls, value):
        for k, v in value.items():
            if v.isdigit():
                value[k] = int(v)
            elif v.startswith(("+", "-")) and v[1:].isdigit():
                value[k] = int(v[1:])
            elif v.replace(".", "", 1).isdigit():
                value[k] = float(v)
            elif v in ["true", "True"]:
                value[k] = True
            elif v in ["false", "False"]:
                value[k] = False
            elif v in ["none", "None", "null"]:
                value[k] = None
        return value


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    response_model=ImageCollection,
    status_code=status.HTTP_200_OK,
)
async def get_dataset_images(
    project_id: int,
    dataset_id: int,
    request: Request,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[SingleImage]:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    images: DatasetV2 = output["dataset"].images

    # Query parameters casting
    query = ImageQuery(
        path=request.query_params.get("path"),
        attributes={
            k: v for k, v in request.query_params.items() if k != "path"
        },
    )

    if query.path is not None:
        images = [
            image
            for image in images
            if image["path"] == request.query_params["path"]
        ]

    if query.attributes is not None:
        images = [
            image
            for image in images
            if SingleImage(**image).match_filter(query.attributes)
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
