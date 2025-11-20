from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import EmailStr
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.routes.api.v2._aux_functions_sharing import (
    get_link_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_sharing import (
    get_pending_invitation_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_sharing import (
    get_user_id_from_email_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_sharing import (
    raise_403_if_not_owner,
)
from fractal_server.app.routes.api.v2._aux_functions_sharing import (
    raise_422_if_link_exists,
)
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.schemas.v2 import ProjectShareCreate
from fractal_server.app.schemas.v2 import ProjectShareReadGuest
from fractal_server.app.schemas.v2 import ProjectShareReadOwner
from fractal_server.app.schemas.v2 import ProjectShareUpdatePermissions

router = APIRouter()


# OWNER


@router.get(
    "/project/{project_id}/link/", response_model=list[ProjectShareReadOwner]
)
async def get_project_guests(
    project_id: int,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectShareReadOwner]:
    """
    Get the list of all the guests of your project (verified or not).
    """
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)
    # Get (email, is_verified, permissions) for all guests
    res = await db.execute(
        select(
            UserOAuth.email,
            LinkUserProjectV2.is_verified,
            LinkUserProjectV2.permissions,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.user_id == UserOAuth.id)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.is_owner.is_(False))
        .order_by(UserOAuth.email)
    )
    guest_tuples = res.all()
    return [
        dict(
            guest_email=guest_email,
            is_verified=is_verified,
            permissions=permissions,
        )
        for guest_email, is_verified, permissions in guest_tuples
    ]


@router.post("/project/{project_id}/link/", status_code=201)
async def share_a_project(
    project_id: int,
    email: EmailStr,
    project_invitation: ProjectShareCreate,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Add a guest to your project.
    """
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    await raise_422_if_link_exists(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    db.add(
        LinkUserProjectV2(
            project_id=project_id,
            user_id=guest_id,
            is_owner=False,
            is_verified=False,
            permissions=project_invitation.permissions,
        )
    )
    await db.commit()

    return Response(status_code=status.HTTP_201_CREATED)


@router.patch("/project/{project_id}/link/", status_code=200)
async def patch_guest_permissions(
    project_id: int,
    email: EmailStr,
    update: ProjectShareUpdatePermissions,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Change guest's permissions on your project.
    """
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    if guest_id == owner.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot perform this operation on project owner.",
        )

    link = await get_link_or_404(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    # Update link and commit
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(link, key, value)
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)


@router.delete("/project/{project_id}/link/", status_code=204)
async def revoke_guest_access(
    project_id: int,
    email: EmailStr,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Remove a guest from your project.
    """
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    if guest_id == owner.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot perform this operation on project owner.",
        )

    link = await get_link_or_404(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    # Delete link and commit
    await db.delete(link)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


# GUEST


@router.get("/project/invitation/", response_model=list[ProjectShareReadGuest])
async def get_pending_invitations(
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectShareReadGuest]:
    """
    See your current invitations.
    """

    owner_subquery = (
        select(
            LinkUserProjectV2.project_id, UserOAuth.email.label("owner_email")
        )
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
        .subquery()
    )

    res = await db.execute(
        select(
            ProjectV2.id,
            ProjectV2.name,
            LinkUserProjectV2.permissions,
            owner_subquery.c.owner_email,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .join(owner_subquery, owner_subquery.c.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_verified.is_(False))
        .order_by(ProjectV2.name)
    )

    guest_project_info = res.all()

    return [
        dict(
            project_id=project_id,
            project_name=project_name,
            guest_permissions=guest_permissions,
            owner_email=owner_email,
        )
        for (
            project_id,
            project_name,
            guest_permissions,
            owner_email,
        ) in guest_project_info
    ]


@router.post("/project/{project_id}/guest-link/accept/", status_code=200)
async def accept_project_invitation(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Accept invitation to project `project_id`.
    """
    link = await get_pending_invitation_or_404(
        user_id=user.id, project_id=project_id, db=db
    )
    link.is_verified = True
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)


@router.delete("/project/{project_id}/guest-link/", status_code=204)
async def delete_guest_link(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Decline invitation to project `project_id` or stop being a guest of that
    project.
    """
    link = await get_link_or_404(user_id=user.id, project_id=project_id, db=db)

    if link.is_owner:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"You are the owner of project {project_id}.",
        )

    await db.delete(link)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
