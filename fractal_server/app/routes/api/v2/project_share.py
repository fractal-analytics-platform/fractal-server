from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.schemas.v2 import ProjectInvitation

router = APIRouter()


async def check_user_is_project_owner(
    *, user_id: int, project_id: int, db: AsyncSession
):
    res = await db.execute(
        select(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    current_user_link = res.scalars().one_or_none()
    if current_user_link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{user_id}' is not the owner of project {project_id}",
        )


@router.post("/linkuserproject/", status_code=201)
async def invite_user_to_project(
    project_id: int,
    project_invitation: ProjectInvitation,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    await check_user_is_project_owner(
        user_id=user.id, project_id=project_id, db=db
    )

    res = await db.execute(
        select(UserOAuth.id).where(
            UserOAuth.email == project_invitation.user_email
        )
    )
    invited_user_id = res.scalar_one_or_none()
    if invited_user_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"User '{project_invitation.user_email}' not found.",
        )

    db.add(
        LinkUserProjectV2(
            project_id=project_id,
            user_id=invited_user_id,
            is_owner=False,
            is_verified=False,
            permissions=project_invitation.permissions,
        )
    )
    await db.commit()

    return


# # Approve pending invitation
# PATCH /api/v2/linkuserproject/set-verified/?project_id=123
# [response only includes a 200 status, with no body]
# [TBD - or perhaps we can use a POST]

# # Reject invitation
# DELETE /api/v2/linkuserproject/?project_id=123
# [204 or 404 or 422 if I am the project owner]
