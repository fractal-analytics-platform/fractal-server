from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import PixiVersion
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import PixiVersionCreate
from fractal_server.app.schemas.v2 import PixiVersionRead

router = APIRouter()


@router.post(
    "/",
    response_model=PixiVersionRead,
    status_code=status.HTTP_201_CREATED,
)
async def post_new_pixi_version(
    pixi_version: PixiVersionCreate,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> PixiVersionRead:
    """
    Add new Pixi version to database
    """
    version = PixiVersion(
        version=pixi_version.version,
        path=pixi_version.path,
    )
    db.add(version)

    try:
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    await db.refresh(version)
    return version


@router.get("/", response_model=list[PixiVersion])
async def get_pixi_version_list(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[PixiVersion]:
    """
    Get the list of available Pixi versions
    """
    stm = select(PixiVersion).order_by(PixiVersion.version.desc())
    res = await db.execute(stm)
    version_list = res.scalars().all()
    return version_list


@router.get("/{version}/", response_model=PixiVersion)
async def get_pixi_version(
    version: str,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> PixiVersion:
    """
    Get single Pixi version
    """
    stm = select(PixiVersion).where(PixiVersion.version == version)
    res = await db.execute(stm)
    version = res.scalars().one_or_none()
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PixiVersion '{version}' not found",
        )
    return version
