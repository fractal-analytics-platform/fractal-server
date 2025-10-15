from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from ._aux_functions import _get_resource_or_404
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ResourceCreate
from fractal_server.app.schemas.v2 import ResourceRead
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

router = APIRouter()


def _check_type_match_or_422(new_resource: ResourceCreate) -> None:
    """
    Handle case where `resource.type != FRACTAL_RUNNER_BACKEND`
    """
    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND != new_resource.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{settings.FRACTAL_RUNNER_BACKEND=} != "
                f"{new_resource.type=}"
            ),
        )


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
    _check_type_match_or_422(resource_create)

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


@router.put(
    "/{resource_id}/",
    response_model=ResourceRead,
    status_code=200,
)
async def put_resource(
    resource_id: int,
    resource_update: ResourceCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Overwrite a single `Resource`.
    """

    # Handle case where type!=FRACTAL_RUNNER_BACKEND
    _check_type_match_or_422(resource_update)

    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    # Handle non-unique resource names
    if (
        resource_update.name is not None
        and resource_update.name != resource.name
    ):
        res = await db.execute(
            select(Resource).where(Resource.name == resource_update.name)
        )
        if res.scalars().one_or_none():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"Resource name '{resource_update.name}' "
                    "already in use."
                ),
            )

    # Prepare new db object
    for key, value in resource_update.model_dump(exclude_unset=True).items():
        setattr(resource, key, value)

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
    await db.delete(resource)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
