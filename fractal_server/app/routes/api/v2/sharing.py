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
    Get the list of all users linked to your project.
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
):
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    # Get the ID of the user to invite
    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    # Check if link already exists
    await raise_422_if_link_exists(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    # Create new link
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
    # FIXME:  maybe return Response(status_code=status.HTTP_201...)

    return


@router.patch("/project/{project_id}/link/", status_code=200)
async def patch_guest_permissions(
    project_id: int,
    email: EmailStr,
    update: ProjectShareUpdatePermissions,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    # Check current user is project owner
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    # Get the ID of the linked user
    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    # Check you're not changing your own permissions
    if guest_id == owner.id:
        raise HTTPException(  # FIXME coverage
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot perform this operation on project owner.",
        )

    # Get the link to update
    link = await get_link_or_404(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    # Update and commit
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(link, key, value)
    await db.commit()

    return  # FIXME add 200 response?


@router.delete("/project/{project_id}/link/", status_code=204)
async def revoke_guest_access(
    project_id: int,
    email: EmailStr,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    await raise_403_if_not_owner(user_id=owner.id, project_id=project_id, db=db)

    guest_id = await get_user_id_from_email_or_404(user_email=email, db=db)

    # Check you're not removing yourself
    if guest_id == owner.id:
        raise HTTPException(  # FIXME coverage
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot perform this operation on project owner.",
        )

    # Get the link to remove
    link = await get_link_or_404(
        user_id=guest_id,
        project_id=project_id,
        db=db,
    )

    # Delete
    await db.delete(link)
    await db.commit()

    return  # FIXME: 204 explicit?


# GUEST


@router.get("/project/invitation/", response_model=list[ProjectShareReadGuest])
async def get_pending_invitations(
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectShareReadGuest]:
    """
    See current invitations
    """

    # FIXME: explore query optimization

    # Get (project_id, project_name, permissions) tuples
    res = await db.execute(
        select(
            ProjectV2.id,
            ProjectV2.name,
            LinkUserProjectV2.permissions,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_verified.is_(False))
        .order_by(ProjectV2.name)
    )

    guest_project_info = res.all()

    # Find owners email
    project_owner_emails = []
    for project_id, project_name, guest_permissions in guest_project_info:
        # Get single project-owner email
        res = await db.execute(
            select(UserOAuth.email)
            .join(LinkUserProjectV2, LinkUserProjectV2.user_id == UserOAuth.id)
            .where(LinkUserProjectV2.project_id == project_id)
            .where(LinkUserProjectV2.is_owner.is_(True))
        )
        project_owner_emails.append(res.scalar_one_or_none())

    return [
        dict(
            project_id=project_id,
            project_name=project_name,
            guest_permissions=guest_permissions,
            owner_email=owner_email,
        )
        for (project_id, project_name, guest_permissions), owner_email in zip(
            guest_project_info, project_owner_emails
        )
    ]


@router.post("/project/{project_id}/guest-link/accept/", status_code=200)
async def accept_project_invitation(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    link = await get_pending_invitation_or_404(
        user_id=user.id, project_id=project_id, db=db
    )
    link.is_verified = True
    db.add(link)  # FIXME: needed?
    await db.commit()

    return  # add response?


@router.delete("/project/{project_id}/guest-link/", status_code=204)
async def delete_guest_link(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    """
    FIXME: mention the two ways to use
    """
    link = await get_link_or_404(user_id=user.id, project_id=project_id, db=db)

    if link.is_owner:
        raise HTTPException(  # FIXME coverage
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"You are the owner of project {project_id}.",
        )

    await db.delete(link)
    await db.commit()

    return  # add response?
