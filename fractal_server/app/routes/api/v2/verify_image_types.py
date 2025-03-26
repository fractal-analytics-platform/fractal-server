from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status

from ._aux_functions import _get_dataset_check_owner
from .images import ImageQuery
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.images.tools import filter_image_list

router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/images/verify-unique-types/",
    status_code=status.HTTP_200_OK,
)
async def verify_unique_types(
    project_id: int,
    dataset_id: int,
    query: Optional[ImageQuery] = None,
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

    # Explore types
    available_types = set(
        _type for _img in filtered_images for _type in _img["types"].keys()
    )
    set_type_value = set(
        (_type, _img["types"].get(_type, False))
        for _img in filtered_images
        for _type in available_types
    )
    values = {}
    for type_value in set_type_value:
        values[type_value[0]] = values.get(type_value[0], []) + [type_value[1]]
    invalid_types = [key for key, value in values.items() if len(value) > 1]
    invalid_types = sorted(invalid_types)

    return invalid_types
