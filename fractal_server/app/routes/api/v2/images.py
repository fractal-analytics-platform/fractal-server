from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from pydantic import BaseModel

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


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/images/",
    response_model=ImageCollection,
    status_code=status.HTTP_200_OK,
)
async def get_dataset_images(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[SingleImage]:

    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset: DatasetV2 = output["dataset"]
    return ImageCollection(
        images=dataset.images,
        attributes=list(
            set(
                [
                    attribute
                    for image in dataset.images
                    for attribute in image["attributes"].keys()
                ]
            )
        ),
    )
