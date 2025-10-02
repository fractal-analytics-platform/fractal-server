from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from .....logger import reset_logger_handlers
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models import LinkUserGroup
from ....models.v2 import JobV2
from ....models.v2 import LinkUserProjectV2
from ....models.v2 import ProjectV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import ProjectInvitation
from ....schemas.v2 import ProjectReadV2
from ....schemas.v2 import ProjectUpdateV2
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _verify_project_access
from ._aux_functions import _verify_project_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@router.get("/project/", response_model=list[ProjectReadV2])
async def get_list_owned_project(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectV2]:
    """
    Return list of owned projects
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.get("/project/shared/", response_model=list[ProjectReadV2])
async def get_list_shared_project(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectV2]:
    """
    Return list of projects shared with the user
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_owner.is_(False))
        .where(LinkUserProjectV2.is_verified.is_(True))
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.post("/project/", response_model=ProjectReadV2, status_code=201)
async def create_project(
    project: ProjectCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectReadV2 | None:
    """
    Create new project
    """

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = ProjectV2(**project.model_dump())
    db.add(db_project)
    await db.commit()
    await db.refresh(db_project)

    link = LinkUserProjectV2(
        project_id=db_project.id,
        user_id=user.id,
        # owner has all permissions
        is_owner=True,
        is_verified=True,
        can_write=True,
        can_execute=True,
    )
    db.add(link)
    await db.commit()

    await db.close()

    return db_project


@router.get("/project/{project_id}/", response_model=ProjectReadV2)
async def read_project(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectReadV2 | None:
    """
    Return info on an existing project
    """

    project = await _verify_project_access(
        project_id=project_id, user_id=user.id, access_type="read", db=db
    )
    await db.close()
    return project


@router.patch("/project/{project_id}/", response_model=ProjectReadV2)
async def update_project(
    project_id: int,
    project_update: ProjectUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    project = await _verify_project_access(
        project_id=project_id, user_id=user.id, access_type="write", db=db
    )

    # Check that there is no project with the same user and name
    if project_update.name is not None:
        await _check_project_exists(
            project_name=project_update.name, user_id=user.id, db=db
        )

    for key, value in project_update.model_dump(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/project/{project_id}/", status_code=204)
async def delete_project(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete project
    """

    project = await _verify_project_access(
        project_id=project_id, user_id=user.id, access_type="write", db=db
    )
    logger = set_logger(__name__)

    # Fail if there exist jobs that are submitted and in relation with the
    # current project.
    stm = _get_submitted_jobs_statement().where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete project {project.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    logger.info(f"Adding Project[{project.id}] to deletion.")
    await db.delete(project)

    logger.info("Committing changes to db...")
    await db.commit()

    logger.info("Everything  has been deleted correctly.")
    reset_logger_handlers(logger)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/project/{project_id}/share/invite-users/", status_code=201)
async def invite_users(
    project_id: int,
    invite: ProjectInvitation,
    can_write: bool = False,
    can_execute: bool = False,
    current_user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    await _verify_project_owner(
        project_id=project_id, user_id=current_user.id, db=db
    )

    if current_user.email in invite.email_list:
        raise HTTPException(status_code=422, detail="Cannot invite yourself")

    res = await db.execute(
        select(UserOAuth).where(UserOAuth.email.in_(invite.email_list))
    )
    invited_users = res.scalars().all()

    # Handle not all users exist
    if len(invite.email_list) != len(invited_users):
        raise HTTPException(status_code=500, detail="FIXME")

    invited_users_ids = {u.email: u.id for u in invited_users}

    # Handle some user is already linked
    res = await db.execute(
        select(LinkUserProjectV2)
        .join(UserOAuth)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == UserOAuth.id)
        .where(UserOAuth.email.in_(invite.email_list))
    )
    existing_links = res.scalars().all()
    if existing_links:
        raise HTTPException(status_code=500, detail="FIXME")

    db.add_all(
        [
            LinkUserProjectV2(
                project_id=project_id,
                user_id=invited_users_ids[email],
                is_owner=False,
                is_verified=False,
                can_write=can_write,
                can_execute=can_execute,
            )
            for email in invite.email_list
        ]
    )
    await db.commit()

    return Response(status_code=status.HTTP_201_CREATED)


@router.post("/project/{project_id}/share/invite-user-group/", status_code=201)
async def invite_user_group(
    project_id: int,
    user_group_id: int,
    can_write: bool = False,
    can_execute: bool = False,
    current_user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    user_membership = await db.get(
        LinkUserGroup, (user_group_id, current_user.id)
    )
    if user_membership is None:
        raise HTTPException(
            status_code=403,
            detail=f"You are not a member of group {user_group_id}",
        )

    res = await db.execute(
        select(UserOAuth.email)
        .join(LinkUserGroup)
        .where(LinkUserGroup.user_id == UserOAuth.id)
        .where(LinkUserGroup.group_id == user_group_id)
    )
    email_list = res.scalas().all()
    email_list.remove(current_user.email)

    return await invite_users(
        project_id=project_id,
        invite=ProjectInvitation(email_list=email_list),
        can_write=can_write,
        can_execute=can_execute,
        current_user=current_user,
        db=db,
    )
