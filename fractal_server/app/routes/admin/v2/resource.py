from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import ValidationError
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _check_resource_name
from ._aux_functions import _get_resource_or_404
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ResourceCreate
from fractal_server.app.schemas.v2 import ResourceRead
from fractal_server.app.schemas.v2 import ResourceUpdate
from fractal_server.app.schemas.v2.resource import validate_resource
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

router = APIRouter()


@router.get("/", response_model=list[ResourceRead], status_code=200)
async def get_resource_list(
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ResourceRead]:
    """
    Query `Resource` table.
    """

    stm = select(Resource)
    res = await db.execute(stm)
    resource_list = res.scalars().all()

    return resource_list


@router.get("/{resource_id}/", response_model=ResourceRead, status_code=200)
async def get_resource(
    resource_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Query single `Resource`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    return resource


@router.post("/", response_model=ResourceRead, status_code=201)
async def post_resource(
    resource_create: ResourceCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Create new `Resource`.
    """

    # Handle case where type!=FRACTAL_RUNNER_BACKEND
    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND != resource_create.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{settings.FRACTAL_RUNNER_BACKEND=} != "
                f"{resource_create.type=}"
            ),
        )

    await _check_resource_name(name=resource_create.name, db=db)

    # Handle non-unique resource names
    res = await db.execute(
        select(Resource).where(Resource.name == resource_create.name)
    )
    if res.scalars().one_or_none():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Resource name '{resource_create.name}' already in use.",
        )

    resource = Resource(**resource_create.model_dump())
    db.add(resource)
    await db.commit()
    await db.refresh(resource)

    return resource


@router.patch(
    "/{resource_id}/",
    response_model=ResourceRead,
    status_code=200,
)
async def patch_resource(
    resource_id: int,
    resource_update: ResourceUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Patch single `Resource`.
    """

    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    # Handle non-unique resource names
    if resource_update.name and resource_update.name != resource.name:
        await _check_resource_name(name=resource_update.name, db=db)

    # Prepare new db object
    for key, value in resource_update.model_dump(exclude_unset=True).items():
        setattr(resource, key, value)

    # Validate new db object
    try:
        validate_resource(resource.model_dump())
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "PATCH would lead to invalid resource. Original error: "
                f"{str(e)}."
            ),
        )

    await db.commit()
    await db.refresh(resource)
    return resource


@router.delete("/{resource_id}/", status_code=204)
async def delete_resource(
    resource_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single `Resource`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    # Fail if at least one Profile is associated with the Resource.
    res = await db.execute(
        select(func.count(Profile.id)).where(
            Profile.resource_id == resource_id
        )
    )
    associated_profile_count = res.scalar()
    if associated_profile_count > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete Resource {resource_id} because it's associated"
                f" with {associated_profile_count} Profiles."
            ),
        )

    # Delete
    await db.delete(resource)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
