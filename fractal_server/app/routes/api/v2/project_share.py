from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic import EmailStr
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.schemas.v2 import ProjectShareCreate
from fractal_server.app.schemas.v2 import ProjectShareRead

router = APIRouter(prefix="/project/share")


async def check_user_is_project_owner(
    *, user_id: int, project_id: int, db: AsyncSession
) -> LinkUserProjectV2:
    res = await db.execute(
        select(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    link = res.scalars().one_or_none()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{user_id}' is not the owner of project {project_id}",
        )
    return link


async def check_user_has_pending_invitation(
    *, user_id: int, project_id: int, db: AsyncSession
) -> LinkUserProjectV2:
    res = await db.execute(
        select(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_verified.is_(False))
    )
    link = res.scalars().one_or_none()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"User '{user_id}' has no pending invitation "
                f"to project {project_id}"
            ),
        )
    return link


async def get_user_id_from_email(
    *, user_email: EmailStr, db: AsyncSession
) -> int:
    res = await db.execute(
        select(UserOAuth.id).where(UserOAuth.email == user_email)
    )
    user_id = res.scalar_one_or_none()
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user_id


async def get_user_email_from_id(
    *, user_id: int, db: AsyncSession
) -> str | None:
    res = await db.execute(
        select(UserOAuth.email).where(UserOAuth.id == user_id)
    )
    user_email = res.scalar_one_or_none()
    if user_email is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user_email


@router.get(
    "/project/{project_id}/link/", response_model=list[ProjectShareRead]
)
async def get_project_linked_users(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectShareRead]:
    """
    Get the list of all users linked to your project.
    """
    # Check current user is project owner
    await check_user_is_project_owner(
        user_id=user.id, project_id=project_id, db=db
    )
    # Get (email, is_verified, permissions) for all linked users except owner
    res = await db.execute(
        select(
            UserOAuth.email,
            LinkUserProjectV2.is_verified,
            LinkUserProjectV2.permissions,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.user_id == UserOAuth.id)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.is_owner.is_(False))
    )
    links = res.scalars().all()

    return [
        ProjectShareRead(
            user_email=link[0],
            is_verified=link[1],
            permissions=link[2],
        )
        for link in links
    ]


@router.post("/project/{project_id}/link/", status_code=201)
async def send_project_invitation(
    project_id: int,
    email: EmailStr,
    project_invitation: ProjectShareCreate,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    # Check current user is project owner
    await check_user_is_project_owner(
        user_id=user.id, project_id=project_id, db=db
    )

    # Get the ID of the user to invite
    invited_user_id = await get_user_id_from_email(user_email=email, db=db)

    # Check if link already exists
    existing_link = await db.get(
        LinkUserProjectV2, (project_id, invited_user_id)
    )
    if existing_link is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Link already exists.",
        )

    # Create new link
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


@router.patch("/accept/", status_code=200)
async def accept_project_invitation(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    link = await check_user_has_pending_invitation(
        user_id=user.id, project_id=project_id, db=db
    )

    link.is_verified = True
    db.add(link)
    await db.commit()
    return


@router.delete("/reject/", status_code=204)
async def reject_project_invitation(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    link = await db.get(LinkUserProjectV2, (project_id, user.id))

    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{user.id}' is not invited to project {project_id}.",
        )

    if link.is_owner:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"You are the owner of project {project_id}.",
        )

    await db.delete(link)
    await db.commit()
    return
