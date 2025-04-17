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
from fractal_server.images.tools import aggregate_types
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
