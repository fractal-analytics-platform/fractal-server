from os.path import normpath
from pathlib import Path

from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import and_
from sqlmodel import asc
from sqlmodel import not_
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.linkuserproject import LinkUserProjectV2
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2.dataset import DatasetV2
from fractal_server.app.models.v2.project import ProjectV2
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)


async def _get_single_user_with_groups(
    user: UserOAuth,
    db: AsyncSession,
) -> UserRead:
    """
    Enrich a user object by filling its `group_ids_names` attribute.

    Args:
        user: The current `UserOAuth` object
        db: Async db session

    Returns:
        A `UserRead` object with `group_ids_names` dict
    """

    settings = Inject(get_settings)

    stm_groups = (
        select(UserGroup)
        .join(LinkUserGroup, LinkUserGroup.group_id == UserGroup.id)
        .where(LinkUserGroup.user_id == user.id)
        .order_by(asc(LinkUserGroup.timestamp_created))
    )
    res = await db.execute(stm_groups)
    groups = res.scalars().unique().all()
    group_ids_names = [(group.id, group.name) for group in groups]

    # Identify the default-group position in the list of groups
    index = next(
        (
            ind
            for ind, group_tuple in enumerate(group_ids_names)
            if group_tuple[1] == settings.FRACTAL_DEFAULT_GROUP_NAME
        ),
        None,
    )
    if (index is None) or (index == 0):
        # Either the default group does not exist, or it is already the first
        # one. No action needed.
        pass
    else:
        # Move the default group to the first position
        default_group = group_ids_names.pop(index)
        group_ids_names.insert(0, default_group)

    # Create dump of `user.oauth_accounts` relationship
    oauth_accounts = [
        oauth_account.model_dump() for oauth_account in user.oauth_accounts
    ]

    return UserRead(
        **user.model_dump(),
        group_ids_names=group_ids_names,
        oauth_accounts=oauth_accounts,
    )


async def _get_single_usergroup_with_user_ids(
    group_id: int, db: AsyncSession
) -> UserGroupRead:
    """
    Get a group, and construct its `user_ids` list.

    Args:
        group_id:
        db:

    Returns:
        `UserGroupRead` object, with `user_ids` attribute populated
        from database.
    """
    group = await _usergroup_or_404(group_id, db)

    # Get all user/group links
    stm_links = select(LinkUserGroup).where(LinkUserGroup.group_id == group_id)
    res = await db.execute(stm_links)
    links = res.scalars().all()
    user_ids = [link.user_id for link in links]

    return UserGroupRead(**group.model_dump(), user_ids=user_ids)


async def _user_or_404(user_id: int, db: AsyncSession) -> UserOAuth:
    """
    Get a user from db, or raise a 404 HTTP exception if missing.

    Args:
        user_id: ID of the user
        db: Async db session
    """
    user = await db.get(UserOAuth, user_id, populate_existing=True)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )
    return user


async def _usergroup_or_404(usergroup_id: int, db: AsyncSession) -> UserGroup:
    user = await db.get(UserGroup, usergroup_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"UserGroup {usergroup_id} not found.",
        )
    return user


async def _get_default_usergroup_id_or_none(db: AsyncSession) -> int | None:
    """
    Return the ID of the group named `"All"`, if `FRACTAL_DEFAULT_GROUP_NAME`
    is set and such group exists. Return `None`, if
    `FRACTAL_DEFAULT_GROUP_NAME=None` or if the `"All"` group does not exist.
    """
    settings = Inject(get_settings)
    stm = select(UserGroup.id).where(
        UserGroup.name == settings.FRACTAL_DEFAULT_GROUP_NAME
    )
    res = await db.execute(stm)
    user_group_id = res.scalars().one_or_none()

    if (
        settings.FRACTAL_DEFAULT_GROUP_NAME is not None
        and user_group_id is None
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"User group '{settings.FRACTAL_DEFAULT_GROUP_NAME}'"
                " not found.",
            ),
        )

    return user_group_id


async def _verify_user_belongs_to_group(
    *, user_id: int, user_group_id: int, db: AsyncSession
):
    stm = (
        select(LinkUserGroup)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id == user_group_id)
    )
    res = await db.execute(stm)
    link = res.scalars().one_or_none()
    if link is None:
        group = await db.get(UserGroup, user_group_id)
        if group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"UserGroup {user_group_id} not found",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"User {user_id} does not belong "
                    f"to UserGroup {user_group_id}"
                ),
            )


async def _check_project_dirs_update(
    *,
    old_project_dirs: list[str],
    new_project_dirs: list[str],
    user_id: int,
    db: AsyncSession,
) -> None:
    """
    Raises 422 if by replacing user's `project_dirs` with new ones we are
    removing the access to a `zarr_dir` used by some dataset.

    Note both `old_project_dirs` and `new_project_dirs` have been
    normalized through `os.path.normpath`, which notably strips any trailing
    `/` character. To be safe, we also re-normalize them within this function.
    """
    # Create a list of all the old project dirs that will lose privileges.
    # E.g.:
    #   old_project_dirs = ["/a", "/b", "/c/d", "/e/f"]
    #   new_project_dirs = ["/a", "/c", "/e/f/g1", "/e/f/g2"]
    #   removed_project_dirs == ["/b", "/e/f"]
    removed_project_dirs = [
        old_project_dir
        for old_project_dir in old_project_dirs
        if not any(
            Path(old_project_dir).is_relative_to(new_project_dir)
            for new_project_dir in new_project_dirs
        )
    ]
    if removed_project_dirs:
        # Query all the `zarr_dir`s linked to the user such that `zarr_dir`
        # starts with one of the project dirs in `removed_project_dirs`.
        stmt = (
            select(DatasetV2.zarr_dir)
            .join(ProjectV2, ProjectV2.id == DatasetV2.project_id)
            .join(
                LinkUserProjectV2,
                LinkUserProjectV2.project_id == ProjectV2.id,
            )
            .where(LinkUserProjectV2.user_id == user_id)
            .where(LinkUserProjectV2.is_verified.is_(True))
            .where(
                or_(
                    *[
                        DatasetV2.zarr_dir.startswith(normpath(old_project_dir))
                        for old_project_dir in removed_project_dirs
                    ]
                )
            )
        )
        if new_project_dirs:
            stmt = stmt.where(
                and_(
                    *[
                        not_(
                            DatasetV2.zarr_dir.startswith(
                                normpath(new_project_dir)
                            )
                        )
                        for new_project_dir in new_project_dirs
                    ]
                )
            )
        res = await db.execute(stmt)

        # Raise 422 if one of the query results is relative to a path in
        # `removed_project_dirs`, but its not relative to any path in
        # `new_project_dirs`.
        if any(
            (
                any(
                    Path(zarr_dir).is_relative_to(old_project_dir)
                    for old_project_dir in removed_project_dirs
                )
                and not any(
                    Path(zarr_dir).is_relative_to(new_project_dir)
                    for new_project_dir in new_project_dirs
                )
            )
            for zarr_dir in res.scalars().all()
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    "You tried updating the user project_dirs, removing "
                    f"{removed_project_dirs}. This operation is not possible, "
                    "because it would make the user loose access to some of "
                    "their dataset zarr directories."
                ),
            )
