from typing import Optional

from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.v2 import TaskGroupV2


async def _verify_non_duplication_constraints(
    db: AsyncSession,
    pkg_name: str,
    user_id: int,
    version: Optional[str],
    user_group_id: Optional[int] = None,
):
    stm = (
        select(TaskGroupV2)
        .where(TaskGroupV2.pkg_name == pkg_name)
        .where(TaskGroupV2.version == version)  # FIXME test with None
        .where(TaskGroupV2.user_id == user_id)
    )
    res = await db.execute(stm)
    duplicate = res.scalars().all()
    if duplicate:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "There is already a TaskGroupV2 with "
                f"({pkg_name=}, {version=}, {user_id=})."
            ),
        )

    if user_group_id is not None:
        stm = (
            select(TaskGroupV2)
            .where(TaskGroupV2.pkg_name == pkg_name)
            .where(TaskGroupV2.version == version)
            .where(TaskGroupV2.user_group_id == user_group_id)
        )
        res = await db.execute(stm)
        duplicate = res.scalars().all()
        if duplicate:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "There is already a TaskGroupV2 with "
                    f"({pkg_name=}, {version=}, {user_group_id=})."
                ),
            )
