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
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.schemas.v2 import ProjectAccessRead
from fractal_server.app.schemas.v2 import ProjectGuestCreate
from fractal_server.app.schemas.v2 import ProjectGuestRead
from fractal_server.app.schemas.v2 import ProjectGuestUpdate
from fractal_server.app.schemas.v2 import ProjectInvitationRead

from ._aux_functions_sharing import get_link_or_404
from ._aux_functions_sharing import get_pending_invitation_or_404
from ._aux_functions_sharing import get_user_id_from_email_or_404
from ._aux_functions_sharing import raise_403_if_not_owner
from ._aux_functions_sharing import raise_422_if_link_exists

router = APIRouter()


@router.get(
    "/project/{project_id}/guest/",
    response_model=list[ProjectGuestRead],
)
async def get_project_guests(
    project_id: int,
    owner: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectGuestRead]:
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
            email=guest_email,
            is_verified=is_verified,
            permissions=permissions,
        )
        for guest_email, is_verified, permissions in guest_tuples
    ]


@router.post("/project/{project_id}/guest/", status_code=201)
async def invite_guest(
    project_id: int,
    email: EmailStr,
    project_invitation: ProjectGuestCreate,
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


@router.patch("/project/{project_id}/guest/", status_code=200)
async def patch_guest(
    project_id: int,
    email: EmailStr,
    update: ProjectGuestUpdate,
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


@router.delete("/project/{project_id}/guest/", status_code=204)
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


@router.get(
    "/project/invitation/",
    response_model=list[ProjectInvitationRead],
)
async def get_pending_invitations(
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectInvitationRead]:
    """
    See your current invitations.
    """

    res = await db.execute(
        select(
            ProjectV2.id,
            ProjectV2.name,
            LinkUserProjectV2.permissions,
            (
                select(UserOAuth.email)
                .join(
                    LinkUserProjectV2,
                    UserOAuth.id == LinkUserProjectV2.user_id,
                )
                .where(LinkUserProjectV2.is_owner.is_(True))
                .where(LinkUserProjectV2.project_id == ProjectV2.id)
                .scalar_subquery()
                .correlate(ProjectV2)
            ),
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
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


@router.get(
    "/project/{project_id}/access/",
    response_model=ProjectAccessRead,
)
async def get_access_info(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectAccessRead:
    """
    Returns information on your relationship with Project[`project_id`].
    """

    res = await db.execute(
        select(
            LinkUserProjectV2.is_owner,
            LinkUserProjectV2.permissions,
            (
                select(UserOAuth.email)
                .join(
                    LinkUserProjectV2,
                    UserOAuth.id == LinkUserProjectV2.user_id,
                )
                .where(LinkUserProjectV2.is_owner.is_(True))
                .where(LinkUserProjectV2.project_id == project_id)
                .scalar_subquery()
            ),
        )
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_verified.is_(True))
    )

    guest_project_info = res.one_or_none()

    if guest_project_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User has no access to project {project_id}.",
        )

    is_owner, permissions, owner_email = guest_project_info

    return dict(
        is_owner=is_owner,
        permissions=permissions,
        owner_email=owner_email,
    )


@router.post("/project/{project_id}/access/accept/", status_code=200)
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


@router.delete("/project/{project_id}/access/", status_code=204)
async def leave_project(
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
