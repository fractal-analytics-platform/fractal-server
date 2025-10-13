from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ResourceCreate
from fractal_server.app.schemas.v2 import ResourceRead
from fractal_server.app.schemas.v2 import ResourceUpdate
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
    resource = await db.get(Resource, resource_id)

    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {resource_id} not found",
        )

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

    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND != resource_create.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{settings.FRACTAL_RUNNER_BACKEND=} != "
                f"{resource_create.type=}"
            ),
        )

    resource = Resource(**resource_create.model_dump())
    db.add(resource)
    await db.commit()
    await db.refresh(resource)

    return resource


@router.patch("/{resource_id}", response_model=ResourceRead, status_code=200)
async def patch_resource(
    resource_id: int,
    resource_update: ResourceUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Patch single `Resource`.
    """

    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND != resource_update.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{settings.FRACTAL_RUNNER_BACKEND=} "
                f"!= {resource_update.type=}"
            ),
        )

    resource = await get_resource(
        resource_id=resource_id, superuser=superuser, db=db
    )
    for key, value in resource_update.model_dump().items():
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
    resource = await get_resource(
        resource_id=resource_id, superuser=superuser, db=db
    )
    await db.delete(resource)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
